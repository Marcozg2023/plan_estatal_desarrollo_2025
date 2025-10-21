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
WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL", "")

# Google Sheets (CSV publicado)
SHEETS_CSV_URL = os.getenv("SHEETS_CSV_URL", "").strip()
SHEETS_FIELD_MUNICIPIO = os.getenv("SHEETS_FIELD_MUNICIPIO", "Municipio").strip()
SHEETS_CACHE_TTL = int(os.getenv("SHEETS_CACHE_TTL_SECONDS", "120"))

# Base de datos local
DB_PATH = os.getenv("DB_PATH", "./chatbot.db")

app = FastAPI(title="Chatbot PED (1 municipio por persona)", version="1.3.2")

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

# ========= Telegram helpers =========
async def send_message(chat_id: int, text: str, parse_mode: Optional[str] = None):
    if not API_URL:
        return
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(f"{API_URL}/sendMessage", json=payload)
            if r.status_code != 200:
                print(f"[send_message] status={r.status_code} body={r.text[:300]}")
    except Exception as e:
        print(f"[send_message] exception: {e}")

def detect_intent(text: str) -> str:
    t = (text or "").strip().lower()
    if t.startswith("/start"): return "start"
    if t.startswith("/ayuda") or t == "ayuda" or t == "/help": return "ayuda"
    if t.startswith("/info") or "plan estatal" in t or "ped" in t: return "info"
    if t.startswith("/reset"): return "reset"
    if t.startswith("/refrescar"): return "refrescar"
    if t.startswith("/id"): return "id"
    if any(w in t for w in ("gracias", "adios", "adiós", "bye", "nos vemos", "hasta luego")):
        return "despedida"
    if re.match(r"^\s*municipio(\s|:)", t):
        return "municipio"
    if any(w in t for w in ("hola", "buenos días", "buenas", "saludos")):
        return "saludo"
    return "fallback"

def extract_municipio(text: str) -> Optional[str]:
    if not text: return None
    m = re.search(r"municipio[:\s]+(.+)$", text, flags=re.I)
    if m: return m.group(1).strip()
    return text.strip()

# ========= Endpoints =========
@app.get("/healthz")
async def healthz():
    return {"ok": True}

@app.post("/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None),
):
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
                "¡Hola! 👋\n"
                "Soy tu asistente para la **Actualización del Plan Estatal de Desarrollo 2025-2028**.\n\n"
                "📍 Escríbeme: *municipio Pachuca* (por ejemplo) para ver su conteo.\n\n"
                f"📊 **Registros totales a nivel estatal: {total}**",
                parse_mode="Markdown"
            )
            return {"ok": True}

        if intent == "ayuda":
            await send_message(
                chat_id,
                "Comandos:\n"
                "• /start  • /ayuda  • /info\n"
                "• /refrescar  • /id  • /reset\n"
                "• municipio <Nombre>  (ej. municipio Tulancingo)\n\n"
                "Importante: solo podrás consultar el primer municipio que elijas en este chat."
            )
            return {"ok": True}

        if intent == "info":
            await send_message(chat_id, "ℹ️ Consulto el conteo por municipio desde una hoja de Google Sheets publicada.")
            return {"ok": True}

        if intent == "refrescar":
            await get_counts_cached(force=True)
            await send_message(chat_id, "🔄 Cache actualizado.")
            return {"ok": True}

        if intent == "id":
            await send_message(chat_id, f"🆔 Tu chat_id es: {chat_id}")
            return {"ok": True}

        if intent == "reset":
            removed = reset_user_municipio(str(chat_id))
            if removed:
                await send_message(chat_id, "✅ Municipio restablecido. Ahora envía: municipio Pachuca")
            else:
                await send_message(chat_id, "No tenías municipio registrado. Envía: municipio Pachuca")
            return {"ok": True}

        if intent == "despedida":
            await send_message(
                chat_id,
                "🙏 *Gracias por tu colaboración y esfuerzo.*\n\n"
                "Tu participación fortalece la actualización del Plan Estatal de Desarrollo 2025-2028.",
                parse_mode="Markdown"
            )
            return {"ok": True}

        if intent == "municipio":
            if not SHEETS_CSV_URL:
                await send_message(chat_id, "⚠️ No tengo configurada la hoja (SHEETS_CSV_URL).")
                return {"ok": True}

            chat_key = str(chat_id)
            ya_registrado = get_user_municipio(chat_key)

            if ya_registrado:
                n = await get_municipio_count(ya_registrado)
                await send_message(
                    chat_id,
                    f"📍 Tu municipio registrado es *{ya_registrado}* y lleva {n} registro(s).\n\n"
                    "Si crees que es un error, usa /reset.",
                    parse_mode="Markdown"
                )
                return {"ok": True}

            nombre = extract_municipio(text or "")
            if not nombre:
                await send_message(chat_id, "Escríbeme así: municipio Pachuca")
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
                parse_mode="Markdown"
            )
            return {"ok": True}

        await send_message(chat_id, "🤔 No entendí tu mensaje. Escribe /ayuda para ver opciones.")
        return {"ok": True}

    except Exception as e:
        print(f"[webhook] error: {e}")
        return {"ok": True}
