# main.py
from __future__ import annotations

import os, re, time, csv, io, sqlite3, unicodedata
from typing import Dict, Any, Optional, Tuple

import httpx
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI(title="Chatbot PED Hidalgo", version="3.1")

# =========================
# Config / Constantes
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").rstrip("/")

# 🔒 Admin (tú)
ADMIN_USER_ID = 1022676234

SHEETS_CSV_URL = os.getenv("SHEETS_CSV_URL", "").strip()
SHEETS_FIELD_MUNICIPIO = os.getenv("SHEETS_FIELD_MUNICIPIO", "Municipio").strip()
SHEETS_CACHE_TTL = int(os.getenv("SHEETS_CACHE_TTL_SECONDS", "120"))
DB_PATH = os.getenv("DB_PATH", "./chatbot.db")

# =========================
# Municipios oficiales (84)
# =========================
MUNICIPIOS_OFICIALES = [
    "Acatlán","Acaxochitlán","Actopan","Agua Blanca de Iturbide","Ajacuba","Alfajayucan",
    "Almoloya","Apan","El Arenal","Atitalaquia","Atlapexco","Atotonilco de Tula",
    "Atotonilco el Grande","Calnali","Cardonal","Cuautepec de Hinojosa","Chapantongo",
    "Chapulhuacán","Chilcuautla","Eloxochitlán","Emiliano Zapata","Epazoyucan",
    "Francisco I. Madero","Huasca de Ocampo","Huautla","Huazalingo","Huehuetla",
    "Huejutla de Reyes","Huichapan","Ixmiquilpan","Jacala de Ledezma","Jaltocán",
    "Juárez Hidalgo","Lolotla","Metepec","San Agustín Metzquititlán","Metztitlán",
    "Mineral del Chico","Mineral del Monte","La Misión","Mixquiahuala de Juárez",
    "Molango de Escamilla","Nicolás Flores","Nopala de Villagrán","Omitlán de Juárez",
    "San Felipe Orizatlán","Pacula","Pachuca de Soto","Pisaflores","Progreso de Obregón",
    "Mineral de la Reforma","San Agustín Tlaxiaca","San Bartolo Tutotepec","San Salvador",
    "Santiago de Anaya","Santiago Tulantepec","Singuilucan","Tasquillo",
    "Tecozautla","Tenango de Doria","Tepeapulco","Tepehuacán de Guerrero",
    "Tepeji del Rio","Tepetitlán","Tetepango","Villa de Tezontepec",
    "Tezontepec de Aldama","Tianguistengo","Tizayuca","Tlahuelilpan","Tlahuiltepa",
    "Tlanalapa","Tlanchinol","Tlaxcoapan","Tolcayuca","Tula de Allende","Tulancingo de Bravo",
    "Xochiatipan","Xochicoatlán","Yahualica","Zacualtipán de Ángeles","Zapotlán de Juárez",
    "Zempoala","Zimapán"
]

# =========================
# DB (SQLite)
# =========================
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS user_municipio (
        chat_id TEXT PRIMARY KEY,
        municipio TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
    conn.close()

init_db()

def set_user_municipio(chat_id: str, municipio: str) -> None:
    conn = db()
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO user_municipio(chat_id, municipio) VALUES (?, ?)", (chat_id, municipio))
    conn.commit()
    conn.close()

def reset_user_municipio(chat_id: str) -> int:
    conn = db()
    c = conn.cursor()
    c.execute("DELETE FROM user_municipio WHERE chat_id = ?", (chat_id,))
    n = c.rowcount
    conn.commit()
    conn.close()
    return n

def get_user_municipio(chat_id: str) -> Optional[str]:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT municipio FROM user_municipio WHERE chat_id = ?", (chat_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

# =========================
# Utilidades: normalización y fuzzy
# =========================
def strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", s or "") if unicodedata.category(ch) != "Mn")

def normalize(s: str) -> str:
    base = strip_accents((s or "").strip().lower())
    return re.sub(r"\s+", " ", base)

def _levenshtein(a: str, b: str) -> int:
    a, b = normalize(a), normalize(b)
    if a == b: return 0
    if not a: return len(b)
    if not b: return len(a)
    prev = list(range(len(b)+1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        for j, cb in enumerate(b, 1):
            curr.append(min(prev[j]+1, curr[j-1]+1, prev[j-1] + (ca != cb)))
        prev = curr
    return prev[-1]

def validar_municipio(user_text: str, max_dist: int = 2) -> Tuple[Optional[str], Optional[str]]:
    t = normalize(user_text)
    for m in MUNICIPIOS_OFICIALES:
        if t == normalize(m):
            return m, None
    mejor, dist = None, 999
    for m in MUNICIPIOS_OFICIALES:
        d = _levenshtein(t, m)
        if d < dist:
            mejor, dist = m, d
    return (None, mejor) if (mejor and dist <= max_dist) else (None, None)

# =========================
# Sheets cache
# =========================
_cache_counts: Dict[str, int] = {}
_cache_last_fetch: float = 0.0

async def fetch_counts_from_sheets() -> Dict[str, int]:
    """
    Descarga el CSV con cache-buster y headers anti-caché
    para forzar datos frescos desde Google Sheets.
    """
    if not SHEETS_CSV_URL:
        return {}
    try:
        bust = str(int(time.time()))
        sep = "&" if "?" in SHEETS_CSV_URL else "?"
        url = f"{SHEETS_CSV_URL}{sep}_={bust}"
        headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"}
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            r = await client.get(url, headers=headers)
        if r.status_code != 200:
            return {}
        content = r.content.decode("utf-8", errors="replace")
    except Exception:
        return {}
    reader = csv.DictReader(io.StringIO(content))
    counts: Dict[str, int] = {}
    for row in reader:
        mun = (row.get(SHEETS_FIELD_MUNICIPIO) or "").strip()
        if mun:
            counts[mun] = counts.get(mun, 0) + 1
    return counts

async def get_counts_cached(force: bool = False) -> Dict[str, int]:
    global _cache_counts, _cache_last_fetch
    now = time.time()
    if force or (now - _cache_last_fetch > SHEETS_CACHE_TTL) or not _cache_counts:
        data = await fetch_counts_from_sheets()
        if data:
            _cache_counts = data
            _cache_last_fetch = now
    return _cache_counts

# =========================
# Telegram helpers
# =========================
def reply_keyboard() -> Dict[str, Any]:
    return {
        "keyboard": [
            [{"text": "Empezar de nuevo"}, {"text": "Actualizar datos"}],
            [{"text": "/ayuda"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True,
    }

def inline_consultar_de_nuevo(muni: str) -> Dict[str, Any]:
    return {"inline_keyboard": [[{"text": "🔄 Consultar de nuevo", "callback_data": f"consultar:{muni}"}]]}

def inline_only_corregir() -> Dict[str, Any]:
    return {"inline_keyboard": [[{"text": "🧹 Corregir municipio", "callback_data": "invalid_reset"}]]}

async def send_message(chat_id: int, text: str,
                       parse_mode: Optional[str] = "Markdown",
                       reply_markup: Optional[Dict[str, Any]] = None):
    if not API_URL:
        return
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode: payload["parse_mode"] = parse_mode
    if reply_markup: payload["reply_markup"] = reply_markup
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            await client.post(f"{API_URL}/sendMessage", json=payload)
    except Exception as e:
        print(f"[send_message] {e}")

# =========================
# Health / Root
# =========================
@app.get("/")
async def root():
    return {"status": "ok", "message": "Servicio del Chatbot PED en línea 🚀", "endpoints": ["/healthz", "/set-webhook", "/delete-webhook", "/webhook"]}

@app.get("/healthz")
async def healthz():
    return {"ok": True}

# =========================
# Webhook Telegram
# =========================
@app.post("/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None),
):
    # valida secret (si se configuró)
    if WEBHOOK_SECRET and x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
        return JSONResponse({"ok": True})

    update = await request.json()

    # --------- Inline callbacks ---------
    callback = update.get("callback_query")
    if callback:
        cb_id = callback.get("id")
        data = callback.get("data") or ""
        message = callback.get("message") or {}
        chat_id = ((message.get("chat") or {}).get("id")) or None
        from_id = (callback.get("from") or {}).get("id")

        async def answer_cb(text: Optional[str] = None, alert: bool = False):
            if not API_URL or not cb_id: return
            payload = {"callback_query_id": cb_id}
            if text: payload.update({"text": text, "show_alert": alert})
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    await client.post(f"{API_URL}/answerCallbackQuery", json=payload)
            except Exception as e:
                print(f"[answer_cb] {e}")

        if data.startswith("consultar:") and chat_id:
            muni = data.split(":", 1)[1]
            counts = await get_counts_cached()
            n = 0
            for k, v in counts.items():
                if normalize(k) == normalize(muni):
                    n = v
                    break
            await answer_cb()
            await send_message(
                chat_id,
                f"🔄 Consulta actualizada para *{muni}*:\n\nActualmente lleva {n} registro(s).",
                reply_markup=inline_consultar_de_nuevo(muni)
            )
            return {"ok": True}

        # 🔒 El botón "Corregir municipio" (invalid_reset) solo puede ejecutarlo el admin
        if data == "invalid_reset" and chat_id:
            if from_id != ADMIN_USER_ID:
                await answer_cb()
                await send_message(chat_id, "🔒 Solo un administrador puede restablecer el municipio de este chat.")
                return {"ok": True}
            reset_user_municipio(str(chat_id))
            await answer_cb()
            await send_message(
                chat_id,
                "🧹 Listo. Vuelve a escribir tu municipio (sin acentos ni mayúsculas exactas, no pasa nada).\n\nEjemplo: *pachuca de soto*",
                reply_markup=reply_keyboard()
            )
            return {"ok": True}

        await answer_cb()
        return {"ok": True}

    # --------- Mensajes de texto ---------
    message = update.get("message") or {}
    text = message.get("text", "")
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    user_id = ((message.get("from") or {}).get("id"))  # para permisos

    if not chat_id or not text:
        return {"ok": True}

    t = text.strip().lower()

    # ---- Comandos / equivalentes de botones ----
    if t in ("empezar de nuevo", "/start"):
        counts = await get_counts_cached()
        total = sum(counts.values()) if counts else 0
        await send_message(
            chat_id,
            "¡Hola! 👋\n"
            "Soy tu asistente para la **Actualización del Plan Estatal de Desarrollo 2025-2028**.\n\n"
            "📍 *Escribe directamente el nombre del municipio*.\n\n"
            "   No importa si omites acentos o mayúsculas. Ej.: `pachuca de soto`.\n\n"
            f"📊 **Registros totales a nivel estatal: {total}**",
            reply_markup=reply_keyboard()
        )
        return {"ok": True}

    if t.startswith("/ayuda") or t == "ayuda":
        await send_message(
            chat_id,
            "🧭 *Menú de ayuda*\n\n"
            "• Para consultar: *escribe solo el nombre del municipio*. Ej.: `pachuca de soto`.\n"
            "• No importa si no pones acentos o mayúsculas.\n"
            "• Para refrescar los datos: *Actualizar datos* o */refrescar*\n"
            "• Para ver tus IDs: */id*\n\n"
            "📌 Primero valido contra el *listado oficial de 84 municipios* (con sugerencias). "
            "Después consulto el CSV; si no hay fila, muestro 0.",
            reply_markup=reply_keyboard()
        )
        return {"ok": True}

    if t in ("actualizar datos", "/refrescar"):
        counts = await get_counts_cached(force=True)
        total = sum(counts.values()) if counts else 0
        await send_message(
            chat_id,
            f"🔄 Cache actualizado. Registros totales: {total}",
            reply_markup=reply_keyboard()
        )
        return {"ok": True}

    if t.startswith("/id"):
        await send_message(chat_id, f"🆔 *user_id*: `{user_id}`\n💬 *chat_id*: `{chat_id}`")
        return {"ok": True}

    if t.startswith("/reset"):
        if user_id == ADMIN_USER_ID:
            parts = text.split()
            target_chat = str(chat_id)
            if len(parts) > 1:
                candidate = parts[1].strip()
                if re.fullmatch(r"-?\d+", candidate):
                    target_chat = candidate
            removed = reset_user_municipio(target_chat)
            msg = (f"✅ Municipio restablecido para chat_id {target_chat}."
                   if removed else f"ℹ️ No había registro para chat_id {target_chat}.")
            await send_message(chat_id, msg)
        else:
            await send_message(chat_id, "⚠️ Este comando es solo para administradores.")
        return {"ok": True}

    # ---- Compatibilidad "municipio ..."
    if t.startswith("municipio"):
        nombre = text.split(" ", 1)[1] if " " in text else ""
        exacto, sugerido = validar_municipio(nombre)
        if not exacto and not sugerido:
            await send_message(
                chat_id,
                f"⚠️ No encontré *{nombre}* en la lista oficial de municipios.\n\n"
                "Verifica la ortografía o corrígelo.",
                reply_markup=inline_only_corregir()
            )
            return {"ok": True}

        oficial = exacto or sugerido
        actual = get_user_municipio(str(chat_id))

        # 🔒 Regla: si ya hay municipio y quieren cambiar a otro, solo admin puede
        if actual and normalize(actual) != normalize(oficial) and user_id != ADMIN_USER_ID:
            await send_message(
                chat_id,
                f"🔒 Este chat ya está asociado a *{actual}*.\n"
                "Solo un administrador puede cambiarlo.",
                reply_markup=inline_consultar_de_nuevo(actual)
            )
            return {"ok": True}

        # Registrar/consultar
        counts = await get_counts_cached()
        n = next((v for k, v in counts.items() if normalize(k) == normalize(oficial)), 0)
        set_user_municipio(str(chat_id), oficial)
        await send_message(
            chat_id,
            f"✅ Registré *{oficial}* para este chat.\n\nActualmente lleva {n} registro(s).",
            reply_markup=inline_consultar_de_nuevo(oficial)
        )
        return {"ok": True}

    # ---- Texto libre: intentar como municipio
    if not t.startswith("/"):
        nombre = text
        if len(normalize(nombre)) >= 3:
            exacto, sugerido = validar_municipio(nombre)
            if exacto or sugerido:
                oficial = exacto or sugerido
                actual = get_user_municipio(str(chat_id))

                # 🔒 Regla: si ya hay municipio y quieren cambiar a otro, solo admin puede
                if actual and normalize(actual) != normalize(oficial) and user_id != ADMIN_USER_ID:
                    await send_message(
                        chat_id,
                        f"🔒 Este chat ya está asociado a *{actual}*.\n"
                        "Solo un administrador puede cambiarlo.",
                        reply_markup=inline_consultar_de_nuevo(actual)
                    )
                    return {"ok": True}

                counts = await get_counts_cached()
                n = next((v for k, v in counts.items() if normalize(k) == normalize(oficial)), 0)
                set_user_municipio(str(chat_id), oficial)
                await send_message(
                    chat_id,
                    f"✅ Registré *{oficial}* para este chat.\n\nActualmente lleva {n} registro(s).",
                    reply_markup=inline_consultar_de_nuevo(oficial)
                )
                return {"ok": True}

    # ---- Despedidas
    if any(w in t for w in ("gracias", "adios", "adiós", "bye", "hasta luego", "nos vemos")):
        await send_message(
            chat_id,
            "🙏 *Gracias por tu colaboración y esfuerzo.*\n\n"
            "Tu participación fortalece la actualización del Plan Estatal de Desarrollo 2025-2028.",
            reply_markup=reply_keyboard()
        )
        return {"ok": True}

    # ---- Fallback
    await send_message(
        chat_id,
        "🤔 No te entendí. Escribe *el nombre del municipio* (por ejemplo `pachuca de soto`) o usa */ayuda*.",
        reply_markup=reply_keyboard()
    )
    return {"ok": True}

# =========================
# Utilería: set/delete webhook
# =========================
@app.get("/set-webhook")
async def set_webhook():
    if not BOT_TOKEN or not WEBHOOK_URL:
        raise HTTPException(status_code=400, detail="Falta TELEGRAM_BOT_TOKEN o WEBHOOK_URL")
    data = {"url": f"{WEBHOOK_URL}/webhook"}
    if WEBHOOK_SECRET:
        data["secret_token"] = WEBHOOK_SECRET
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(f"{API_URL}/setWebhook", json=data)
        return r.json()

@app.get("/delete-webhook")
async def delete_webhook():
    if not BOT_TOKEN:
        raise HTTPException(status_code=400, detail="Falta TELEGRAM_BOT_TOKEN")
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(f"{API_URL}/deleteWebhook")
        return r.json()
