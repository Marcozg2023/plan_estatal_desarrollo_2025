from __future__ import annotations

import os, re, time, csv, io, sqlite3
from typing import Dict, Any, Optional

import httpx
from fastapi import FastAPI, Request, Header, HTTPException, Query
from fastapi.responses import JSONResponse

# ========= Variables de entorno =========
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL", "")  # URL completa pública hacia /webhook

# Google Sheets (CSV publicado)
SHEETS_CSV_URL = os.getenv("SHEETS_CSV_URL", "").strip()  # pub?output=csv&gid=... o export?format=csv&gid=...
SHEETS_FIELD_MUNICIPIO = os.getenv("SHEETS_FIELD_MUNICIPIO", "Municipio").strip()
SHEETS_CACHE_TTL = int(os.getenv("SHEETS_CACHE_TTL_SECONDS", "120"))

# Base de datos local
DB_PATH = os.getenv("DB_PATH", "./chatbot.db")

app = FastAPI(title="Chatbot PED (1 municipio por persona)", version="1.3.0")

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
    """
    Descarga el CSV del Sheet con follow_redirects=True para tolerar 307/308.
    NO usamos raise_for_status para no romper el webhook; si no es 200 devolvemos {} y logeamos.
    """
    if not SHEETS_CSV_URL:
        return {}
    try:
        async with httpx.AsyncClient(
            timeout=60,
            follow_redirects=True,
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

    # detectar columna de municipio (tolerante)
    headers_map = { (h or "").lower().strip(): (h or "") for h in (reader.fieldnames or []) }
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
        if data:   # solo pisa cache si la lectura fue exitosa
            _cache_counts = data
            _cache_last_fetch = now
        else:
            print("[cache] usando último cache válido (si existe)")
    return _cache_counts

async def get_municipio_count(nombre: str) -> int:
    counts = await get_counts_cached()
    tgt = normalize(nombre)
    # exacto
    for k, v in counts.items():
        if normalize(k) == tgt:
            return v
    # aproximado
    for k, v in counts.items():
        if tgt and tgt in normalize(k):
            return v
    return 0

# ========= Telegram helpers =========
async def send_message(chat_id: int, text: str):
    if not API_URL:
        return
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(f"{API_URL}/sendMessage", json={"chat_id": chat_id, "text": text})
            if r.status_code != 200:
                print(f"[send_message] status={r.status_code} body={r.text[:300]}")
    except Exception as e:
        print(f"[send_message] exception: {e}")

def detect_intent(text: str) -> str:
    t = (text or "").strip().lower()

    # comandos primero
    if t.startswith("/start"): return "start"
    if t.startswith("/ayuda") or t == "ayuda" or t == "/help": return "ayuda"
    if t.startswith("/info") or "plan estatal" in t or "ped" in t: return "info"
    if t.startswith("/reset"): return "reset"
    if t.startswith("/refrescar"): return "refrescar"
    if t.startswith("/id"): return "id"

    # solo "municipio" si inicia con esa palabra
    if re.match(r"^\s*municipio(\s|:)", t):
        return "municipio"

    # saludos
    if any(w in t for w in ("hola", "buenos días", "buenas", "saludos")):
        return "saludo"

    return "fallback"

def extract_municipio(text: str) -> Optional[str]:
    if not text: return None
    m = re.search(r"municipio[:\s]+(.+)$", text, flags=re.I)
    if m: return m.group(1).strip()
    return text.strip()

# ========= Endpoints =========
@app.get("/")
async def home():
    return {
        "status": "ok",
        "one_municipio_per_user": True,
        "sheets_url_set": bool(SHEETS_CSV_URL),
        "field_municipio": SHEETS_FIELD_MUNICIPIO,
        "cache_ttl": SHEETS_CACHE_TTL,
        "db": os.path.abspath(DB_PATH),
        "cache_age_sec": (time.time() - _cache_last_fetch) if _cache_last_fetch else None,
    }

@app.get("/healthz")
async def healthz():
    return {"ok": True}

# Admin: forzar refresco del CSV
@app.post("/refresh-sheets")
async def refresh_sheets():
    counts = await get_counts_cached(force=True)
    return {"ok": True, "municipios": len(counts)}

# Admin: consultar municipio registrado de un chat
@app.get("/admin/user-municipio")
def admin_get_user_municipio(chat_id: str = Query(..., description="chat_id numérico de Telegram")):
    mun = get_user_municipio(str(chat_id))
    return {"chat_id": chat_id, "municipio": mun}

# Admin: resetear municipio de un chat
@app.post("/admin/reset-user-municipio")
def admin_reset_user_municipio(chat_id: str = Query(...)):
    removed = reset_user_municipio(str(chat_id))
    return {"chat_id": chat_id, "reset": bool(removed)}

# Webhook de Telegram
@app.post("/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None),
):
    # valida secret si aplicable (pero no rompas el endpoint)
    if WEBHOOK_SECRET and x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
        print("[webhook] invalid secret token")
        return JSONResponse({"ok": True})

    try:
        update = await request.json()
        message = update.get("message") or {}
        text = message.get("text")
        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        if not chat_id:
            return {"ok": True}

        intent = detect_intent(text or "")

        if intent == "start":
            counts = await get_counts_cached()
            total = sum(counts.values()) if counts else 0
            await send_message(
                chat_id,
                "¡Hola! Soy tu asistente del Plan Estatal de Desarrollo.\n"
                "Escríbeme *municipio Pachuca* (por ejemplo) para ver su conteo.\n"
                f"Registros totales actuales: {total}"
            )
            return {"ok": True}

        if intent == "ayuda":
            await send_message(
                chat_id,
                "Comandos:\n"
                "• /start  • /ayuda  • /info\n"
                "• /refrescar  • /id  • /reset\n"
                "• municipio <Nombre>  (ej. *municipio Tulancingo*)\n"
                "Importante: Podrás consultar *solo el primer municipio* que elijas en este chat."
            )
            return {"ok": True}

        if intent == "info":
            await send_message(chat_id, "Consulto el conteo por municipio desde una hoja de Google Sheets publicada.")
            return {"ok": True}

        if intent == "refrescar":
            await get_counts_cached(force=True)
            await send_message(chat_id, "🔄 Cache actualizado.")
            return {"ok": True}

        if intent == "id":
            await send_message(chat_id, f"Tu chat_id es: {chat_id}")
            return {"ok": True}

        if intent == "reset":
            removed = reset_user_municipio(str(chat_id))
            if removed:
                await send_message(chat_id, "✅ Se restableció tu municipio. Ahora envía: *municipio Pachuca*")
            else:
                await send_message(chat_id, "No tenías municipio registrado. Envía: *municipio Pachuca*")
            return {"ok": True}

        if intent == "municipio":
            if not SHEETS_CSV_URL:
                await send_message(chat_id, "Aún no tengo configurada la hoja (SHEETS_CSV_URL).")
                return {"ok": True}

            chat_key = str(chat_id)
            ya_registrado = get_user_municipio(chat_key)

            if ya_registrado:
                n = await get_municipio_count(ya_registrado)
                await send_message(
                    chat_id,
                    f"Tu municipio registrado es *{ya_registrado}* y lleva {n} registro(s).\n"
                    "Si crees que es un error, usa /reset o solicita a un administrador que lo restablezca."
                )
                return {"ok": True}

            nombre = extract_municipio(text or "")
            if not nombre:
                await send_message(chat_id, "Escríbeme así: *municipio Pachuca*")
                return {"ok": True}

            n = await get_municipio_count(nombre)
            counts = await get_counts_cached()
            elegido = nombre
            for k in counts.keys():
                if normalize(k) == normalize(nombre) or normalize(nombre) in normalize(k):
                    elegido = k
                    break

            set_user_municipio(chat_key, elegido)
            await send_message(chat_id, f"Listo ✅. Registré *{elegido}* para este chat.\nActualmente lleva {n} registro(s).")
            return {"ok": True}

        # fallback
        await send_message(chat_id, "No entendí tu mensaje 🤔. Escribe *ayuda* para ver opciones.")
        return {"ok": True}

    except Exception as e:
        print(f"[webhook] error: {e}")  # Loguea, pero no rompas
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

# Aceptar GET y POST para poder usar navegador o POSTman
@app.api_route("/set-webhook", methods=["GET", "POST"])
async def set_webhook():
    if not WEBHOOK_URL:
        raise HTTPException(status_code=400, detail="Define TELEGRAM_WEBHOOK_URL")
    return await _tg_set_webhook(WEBHOOK_URL, WEBHOOK_SECRET)

@app.api_route("/delete-webhook", methods=["GET", "POST"])
async def delete_webhook():
    return await _tg_delete_webhook()
