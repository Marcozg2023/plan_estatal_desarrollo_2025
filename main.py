from __future__ import annotations

import os, re, time, csv, io, sqlite3, math
from typing import Dict, Any, Optional, List

import httpx
from fastapi import FastAPI, Request, Header, HTTPException, Query
from fastapi.responses import JSONResponse

# ========= Variables de entorno =========
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL", "")  # URL pública completa a /webhook
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))  # tu chat_id para /reset

# Google Sheets (CSV publicado)
SHEETS_CSV_URL = os.getenv("SHEETS_CSV_URL", "").strip()
SHEETS_FIELD_MUNICIPIO = os.getenv("SHEETS_FIELD_MUNICIPIO", "Municipio").strip()
SHEETS_CACHE_TTL = int(os.getenv("SHEETS_CACHE_TTL_SECONDS", "120"))

# Base de datos local
DB_PATH = os.getenv("DB_PATH", "./chatbot.db")

app = FastAPI(title="Chatbot PED (1 municipio por persona)", version="1.3.7")

# ========= DB (SQLite) =========
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
    )
    """)
    conn.commit()
    conn.close()

init_db()

def get_user_municipio(chat_id: str) -> Optional[str]:
    conn = db()
    c = conn.cursor()
    c.execute("SELECT municipio FROM user_municipio WHERE chat_id = ?", (chat_id,))
    row = c.fetchone()
    conn.close()
    return row["municipio"] if row else None

def set_user_municipio(chat_id: str, municipio: str) -> None:
    conn = db()
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO user_municipio(chat_id, municipio) VALUES (?, ?)", (chat_id, municipio))
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

# ========= Caché CSV =========
_cache_counts: Dict[str, int] = {}
_cache_last_fetch: float = 0.0

def normalize(s: str) -> str:
    return (s or "").strip().lower()

async def fetch_counts_from_sheets() -> Dict[str, int]:
    """Descarga CSV con redirects permitidos y sin raise_for_status."""
    if not SHEETS_CSV_URL:
        return {}
    try:
        async with httpx.AsyncClient(
            timeout=60, follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"}
        ) as client:
            r = await client.get(SHEETS_CSV_URL)
        if r.status_code != 200:
            print(f"[sheets] non-200 status={r.status_code} url={r.url}")
            return {}
        content = r.content.decode("utf-8", errors="replace")
    except Exception as e:
        print(f"[sheets] exception: {e}")
        return {}

    reader = csv.DictReader(io.StringIO(content))
    headers_map = {(h or "").lower().strip(): (h or "") for h in (reader.fieldnames or [])}
    field_actual = headers_map.get(SHEETS_FIELD_MUNICIPIO.lower())
    if not field_actual:
        for k, v in headers_map.items():
            if SHEETS_FIELD_MUNICIPIO.lower() in k:
                field_actual = v
                break
    if not field_actual:
        print(f"[sheets] columna '{SHEETS_FIELD_MUNICIPIO}' no encontrada. Headers: {reader.fieldnames}")
        return {}

    counts: Dict[str, int] = {}
    for row in reader:
        mun = (row.get(field_actual) or "").strip()
        if not mun:
            mun = "(Sin municipio)"
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
        else:
            print("[cache] usando último cache válido (si existe)")
    return _cache_counts

async def get_municipio_count(nombre: str) -> int:
    counts = await get_counts_cached()
    tgt = normalize(nombre)
    for k, v in counts.items():
        if normalize(k) == tgt:
            return v
    for k, v in counts.items():
        if tgt and tgt in normalize(k):
            return v
    return 0

# ========= Telegram: teclados & helpers =========
def menu_keyboard() -> Dict[str, Any]:
    """Reply keyboard persistente (SIN municipio ejemplo NI /reset)."""
    return {
        "keyboard": [
            [{"text": "/ayuda"}, {"text": "/refrescar"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True
    }

def remove_keyboard() -> Dict[str, Any]:
    return {"remove_keyboard": True}

def inline_consultar_de_nuevo(municipio: str) -> Dict[str, Any]:
    return {
        "inline_keyboard": [[
            {"text": "🔄 Consultar de nuevo", "callback_data": f"consultar:{municipio}"}
        ]]
    }

def chunk(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def municipios_keyboard(page: int, per_page: int = 24) -> Dict[str, Any]:
    """
    Crea un reply keyboard con botones 'municipio <Nombre>' a partir del CSV.
    Muestra 'per_page' municipios por página (2 por fila).
    Agrega navegación con /municipios <n>.
    """
    # ordenar alfabéticamente (títulos originales)
    nombres = sorted(list(_cache_counts.keys())) if _cache_counts else []
    total = len(nombres)
    if total == 0:
        return menu_keyboard()

    # paginación
    total_pages = max(1, math.ceil(total / per_page))
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    end = start + per_page
    subset = nombres[start:end]

    # filas de 2 botones
    rows = []
    for row in chunk(subset, 2):
        rows.append([{"text": f"municipio {name}"} for name in row])

    # fila de navegación
    nav_row = []
    if page > 1:
        nav_row.append({"text": f"/municipios {page-1}"})
    nav_row.append({"text": "/ayuda"})
    nav_row.append({"text": "/refrescar"})
    if page < total_pages:
        nav_row.append({"text": f"/municipios {page+1}"})
    rows.append(nav_row)

    return {
        "keyboard": rows,
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True
    }

async def send_message(chat_id: int, text: str,
                       parse_mode: Optional[str] = None,
                       reply_markup: Optional[Dict[str, Any]] = None):
    if not API_URL:
        return
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(f"{API_URL}/sendMessage", json=payload)
            if r.status_code != 200:
                print(f"[send_message] status={r.status_code} body={r.text[:300]}")
    except Exception as e:
        print(f"[send_message] exception: {e}")

async def answer_callback(callback_id: str, text: Optional[str] = None, show_alert: bool = False):
    if not API_URL or not callback_id:
        return
    payload = {"callback_query_id": callback_id}
    if text:
        payload["text"] = text
        payload["show_alert"] = show_alert
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(f"{API_URL}/answerCallbackQuery", json=payload)
    except Exception as e:
        print(f"[answer_callback] exception: {e}")

def detect_intent(text: str) -> str:
    t = (text or "").strip().lower()
    if t.startswith("/start"): return "start"
    if (
        t.startswith("/ayuda") or t == "ayuda" or t == "/help" or
        t == "menu" or t == "menú" or "menu de ayuda" in t or "menú de ayuda" in t or
        t == "opciones" or "qué puedo hacer" in t or "que puedo hacer" in t
    ): return "ayuda"
    if t.startswith("/municipios") or t == "municipios": return "municipios"
    if t.startswith("/info") or "plan estatal" in t or "ped" in t: return "info"
    if t.startswith("/reset"): return "reset"  # solo admin
    if t.startswith("/refrescar"): return "refrescar"
    if t.startswith("/id"): return "id"
    if t.startswith("/ocultar"): return "ocultar"
    if any(w in t for w in ("gracias", "adios", "adiós", "bye", "nos vemos", "hasta luego")):
        return "despedida"
    if re.match(r"^\s*municipio(\s|:)", t): return "municipio"
    if any(w in t for w in ("hola", "buenos días", "buenas", "saludos")): return "saludo"
    return "fallback"

def extract_municipio(text: str) -> Optional[str]:
    if not text: return None
    m = re.search(r"municipio[:\s]+(.+)$", text, flags=re.I)
    if m: return m.group(1).strip()
    return text.strip()

def extract_page(text: str) -> int:
    """Lee /municipios <n> (por defecto 1)."""
    m = re.search(r"/municipios\s+(\d+)", (text or "").strip(), flags=re.I)
    if m:
        try:
            return max(1, int(m.group(1)))
        except ValueError:
            return 1
    return 1

# ========= Endpoints =========
@app.get("/")
async def home():
    return {
        "status": "ok",
        "message": "Servicio del Chatbot PED en línea 🚀",
        "endpoints": ["/healthz", "/webhook", "/set-webhook", "/delete-webhook"]
    }

@app.get("/healthz")
async def healthz():
    return {"ok": True}

@app.post("/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None),
):
    # valida secret si se configuró (pero no rompas el endpoint)
    if WEBHOOK_SECRET and x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
        print("[webhook] invalid secret token")
        return JSONResponse({"ok": True})

    try:
        update = await request.json()

        # --- Callback queries (inline buttons) ---
        callback = update.get("callback_query")
        if callback:
            callback_id = callback.get("id")
            data = callback.get("data") or ""
            chat_id = ((callback.get("message") or {}).get("chat") or {}).get("id")

            if data.startswith("consultar:") and chat_id:
                muni = data.split(":", 1)[1]
                n = await get_municipio_count(muni)
                await answer_callback(callback_id)
                await send_message(
                    chat_id,
                    f"🔄 Consulta actualizada para *{muni}*:\n\nActualmente lleva {n} registro(s).",
                    parse_mode="Markdown",
                    reply_markup=inline_consultar_de_nuevo(muni)
                )
            else:
                await answer_callback(callback_id)
            return {"ok": True}

        # --- Mensajes normales ---
        message = update.get("message") or {}
        text = message.get("text")
        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        if not chat_id:
            return {"ok": True}

        intent = detect_intent(text or "")

        # /start
        if intent == "start":
            counts = await get_counts_cached()
            total = sum(counts.values()) if counts else 0
            await send_message(
                chat_id,
                "¡Hola! 👋\n"
                "Soy tu asistente para la **Actualización del Plan Estatal de Desarrollo 2025-2028**.\n\n"
                "📍 Escríbeme: *municipio Pachuca* (por ejemplo) para ver su conteo.\n"
                "También puedes usar */municipios* para ver botones rápidos por nombre.\n\n"
                f"📊 **Registros totales a nivel estatal: {total}**",
                parse_mode="Markdown",
                reply_markup=menu_keyboard()
            )
            return {"ok": True}

        # /ayuda (sin /reset), referencia /municipios
        if intent == "ayuda":
            ayuda_text = (
                "🧭 *Menú de ayuda*\n\n"
                "¿Qué puedes hacer aquí?\n\n"
                "1) 👀 Consultar el conteo de tu municipio\n"
                "   Escribe: *municipio Pachuca*\n"
                "   (o usa */municipios* para ver botones rápidos)\n\n"
                "2) 📊 Ver el total estatal actualizado\n"
                "   Escribe: */start*\n\n"
                "3) 🔄 Actualizar los datos (si cambió la base)\n"
                "   Escribe: */refrescar*\n\n"
                "4) 🆔 Ver tu ID de chat (por soporte)\n"
                "   Escribe: */id*\n\n"
                "5) 🆘 Volver a este menú\n"
                "   Escribe: */ayuda*\n\n"
                "—\n"
                "📌 *Notas importantes*\n"
                "• Solo se registra *un municipio por chat*. Si necesitas cambiarlo, contacta a un *administrador*.\n"
                "• Los datos se leen de una hoja pública de Google Sheets y se *actualizan cada 1–2 minutos*.\n"
                "• No solicitamos datos personales. Tu participación ayuda a fortalecer la planeación del estado."
            )
            await send_message(chat_id, ayuda_text, parse_mode="Markdown", reply_markup=menu_keyboard())
            return {"ok": True}

        # /municipios [página]
        if intent == "municipios":
            # aseguramos cache al día
            await get_counts_cached()
            page = extract_page(text or "")
            kb = municipios_keyboard(page=page, per_page=24)
            total = len(_cache_counts)
            await send_message(
                chat_id,
                f"📚 Municipios disponibles (página {page}). Total: {total}.\n"
                "Toca un botón para consultar. Usa /municipios <n> para cambiar de página.",
                reply_markup=kb
            )
            return {"ok": True}

        # /info
        if intent == "info":
            await send_message(
                chat_id,
                "ℹ️ Consulto el conteo por municipio desde una hoja de Google Sheets publicada.",
                reply_markup=menu_keyboard()
            )
            return {"ok": True}

        # /refrescar
        if intent == "refrescar":
            await get_counts_cached(force=True)
            await send_message(chat_id, "🔄 Cache actualizado.", reply_markup=menu_keyboard())
            return {"ok": True}

        # /id
        if intent == "id":
            await send_message(chat_id, f"🆔 Tu chat_id es: {chat_id}", reply_markup=menu_keyboard())
            return {"ok": True}

        # /ocultar (remueve el teclado base)
        if intent == "ocultar":
            await send_message(chat_id, "Teclado ocultado. Para mostrarlo otra vez envía /ayuda o /start.", reply_markup=remove_keyboard())
            return {"ok": True}

        # /reset (SOLO ADMIN)
        if intent == "reset":
            if ADMIN_CHAT_ID == 0 or chat_id != ADMIN_CHAT_ID:
                await send_message(chat_id, "⚠️ Este comando solo está disponible para administradores.")
                return {"ok": True}
            removed = reset_user_municipio(str(chat_id))
            if removed:
                await send_message(chat_id, "✅ Municipio restablecido.")
            else:
                await send_message(chat_id, "No tenías municipio registrado.")
            return {"ok": True}

        # despedida
        if intent == "despedida":
            await send_message(
                chat_id,
                "🙏 *Gracias por tu colaboración y esfuerzo.*\n\n"
                "Tu participación fortalece la actualización del Plan Estatal de Desarrollo 2025-2028.",
                parse_mode="Markdown",
                reply_markup=menu_keyboard()
            )
            return {"ok": True}

        # municipio
        if intent == "municipio":
            if not SHEETS_CSV_URL:
                await send_message(chat_id, "⚠️ No tengo configurada la hoja (SHEETS_CSV_URL).", reply_markup=menu_keyboard())
                return {"ok": True}

            chat_key = str(chat_id)
            ya_registrado = get_user_municipio(chat_key)

            if ya_registrado:
                n = await get_municipio_count(ya_registrado)
                await send_message(
                    chat_id,
                    f"📍 Tu municipio registrado es *{ya_registrado}* y lleva {n} registro(s).\n\n"
                    "Si necesitas cambiarlo, por favor contacta a un *administrador*.",
                    parse_mode="Markdown",
                    reply_markup=inline_consultar_de_nuevo(ya_registrado)
                )
                return {"ok": True}

            nombre = extract_municipio(text or "")
            if not nombre:
                await send_message(chat_id, "Escríbeme así: municipio Pachuca", reply_markup=menu_keyboard())
                return {"ok": True}

            n = await get_municipio_count(nombre)
            counts = await get_counts_cached()
            elegido = nombre
            for k in counts.keys():
                if normalize(k) == normalize(nombre) or normalize(nombre) in normalize(k):
                    elegido = k
                    break

            set_user_municipio(chat_key, elegido)
            await send_message(
                chat_id,
                f"✅ Registré *{elegido}* para este chat.\n\nActualmente lleva {n} registro(s).",
                parse_mode="Markdown",
                reply_markup=inline_consultar_de_nuevo(elegido)
            )
            return {"ok": True}

        # fallback
        await send_message(chat_id, "🤔 No entendí tu mensaje. Escribe /ayuda o usa /municipios.", reply_markup=menu_keyboard())
        return {"ok": True}

    except Exception as e:
        print(f"[webhook] error: {e}")
        return {"ok": True}

# ========= Utilidades set/delete webhook =========
async def _tg_set_webhook(url: str, secret: str = "") -> Dict[str, Any]:
    if not API_URL:
        raise HTTPException(status_code=400, detail="BOT_TOKEN no configurado")
    data = {"url": url}
    if secret:
        data["secret_token"] = secret
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(f"{API_URL}/setWebhook", data=data)
        return r.json()

async def _tg_delete_webhook() -> Dict[str, Any]:
    if not API_URL:
        raise HTTPException(status_code=400, detail="BOT_TOKEN no configurado")
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(f"{API_URL}/deleteWebhook")
        return r.json()

@app.api_route("/set-webhook", methods=["GET", "POST"])
async def set_webhook():
    if not WEBHOOK_URL:
        raise HTTPException(status_code=400, detail="Define TELEGRAM_WEBHOOK_URL")
    return await _tg_set_webhook(WEBHOOK_URL, WEBHOOK_SECRET)

@app.api_route("/delete-webhook", methods=["GET", "POST"])
async def delete_webhook():
    return await _tg_delete_webhook()
