from __future__ import annotations

import os, re, time, csv, io, sqlite3
from typing import Dict, Any, Optional, List

import httpx
from fastapi import FastAPI, Request, Header
from fastapi.responses import JSONResponse

# ========= Variables de entorno =========
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
WEBHOOK_URL = os.getenv("TELEGRAM_WEBHOOK_URL", "")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))

SHEETS_CSV_URL = os.getenv("SHEETS_CSV_URL", "").strip()
SHEETS_FIELD_MUNICIPIO = os.getenv("SHEETS_FIELD_MUNICIPIO", "Municipio").strip()
SHEETS_CACHE_TTL = int(os.getenv("SHEETS_CACHE_TTL_SECONDS", "120"))
DB_PATH = os.getenv("DB_PATH", "./chatbot.db")

app = FastAPI(title="Chatbot PED Hidalgo", version="2.2")

# ========= Listado oficial =========
MUNICIPIOS_OFICIALES = [
    "Acatlán", "Acaxochitlán", "Actopan", "Agua Blanca de Iturbide",
    "Ajacuba", "Alfajayucan", "Almoloya", "Apan", "El Arenal", "Atitalaquia",
    "Atlapexco", "Atotonilco de Tula", "Atotonilco el Grande", "Calnali",
    "Cardonal", "Cuautepec de Hinojosa", "Chapantongo", "Chapulhuacán",
    "Chilcuautla", "Eloxochitlán", "Emiliano Zapata", "Epazoyucan",
    "Francisco I. Madero", "Huasca de Ocampo", "Huautla", "Huazalingo",
    "Huehuetla", "Huejutla de Reyes", "Huichapan", "Ixmiquilpan",
    "Jacala de Ledezma", "Jaltocán", "Juárez Hidalgo", "Lolotla",
    "Metepec", "San Agustín Metzquititlán", "Metztitlán", "Mineral del Chico",
    "Mineral del Monte", "La Misión", "Mixquiahuala de Juárez",
    "Molango de Escamilla", "Nicolás Flores", "Nopala de Villagrán",
    "Omitlán de Juárez", "San Felipe Orizatlán", "Pacula", "Pachuca de Soto",
    "Pisaflores", "Progreso de Obregón", "Mineral de la Reforma",
    "San Agustín Tlaxiaca", "San Bartolo Tutotepec", "San Salvador",
    "Santiago de Anaya", "Santiago Tulantepec de Lugo Guerrero",
    "Singuilucan", "Tasquillo", "Tecozautla", "Tenango de Doria",
    "Tepeapulco", "Tepehuacán de Guerrero", "Tepeji del Río de Ocampo",
    "Tepetitlán", "Tetepango", "Villa de Tezontepec", "Tezontepec de Aldama",
    "Tianguistengo", "Tizayuca", "Tlahuelilpan", "Tlahuiltepa",
    "Tlanalapa", "Tlanchinol", "Tlaxcoapan", "Tolcayuca", "Tula de Allende",
    "Tulancingo de Bravo", "Xochiatipan", "Xochicoatlán", "Yahualica",
    "Zacualtipán de Ángeles", "Zapotlán de Juárez", "Zempoala", "Zimapán"
]

# ========= DB =========
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

# ========= Cache CSV =========
_cache_counts: Dict[str, int] = {}
_cache_last_fetch: float = 0.0

def normalize(s: str) -> str:
    return (s or "").strip().lower()

async def fetch_counts_from_sheets() -> Dict[str, int]:
    if not SHEETS_CSV_URL:
        return {}
    try:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            r = await client.get(SHEETS_CSV_URL)
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

async def get_counts_cached(force=False) -> Dict[str, int]:
    global _cache_counts, _cache_last_fetch
    now = time.time()
    if force or (now - _cache_last_fetch > SHEETS_CACHE_TTL) or not _cache_counts:
        data = await fetch_counts_from_sheets()
        if data:
            _cache_counts = data
            _cache_last_fetch = now
    return _cache_counts

# ========= Fuzzy matching =========
def _levenshtein(a: str, b: str) -> int:
    a, b = a.lower(), b.lower()
    if a == b: return 0
    if not a: return len(b)
    if not b: return len(a)
    prev = list(range(len(b)+1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        for j, cb in enumerate(b, 1):
            ins = prev[j] + 1
            dele = curr[j-1] + 1
            sub = prev[j-1] + (ca != cb)
            curr.append(min(ins, dele, sub))
        prev = curr
    return prev[-1]

def sugerir_municipio(user_text: str, max_dist=2) -> Optional[str]:
    mejor, dist = None, 999
    for m in MUNICIPIOS_OFICIALES:
        d = _levenshtein(user_text, m)
        if d < dist:
            mejor, dist = m, d
    return mejor if dist <= max_dist else None

def validar_municipio(user_text: str) -> tuple[Optional[str], Optional[str]]:
    """Devuelve (coincidencia_exacta, sugerencia)"""
    t = user_text.strip().lower()
    for m in MUNICIPIOS_OFICIALES:
        if t == m.lower():
            return m, None
    sugerido = sugerir_municipio(t)
    if sugerido:
        return None, sugerido
    return None, None

# ========= Telegram helpers =========
def inline_consultar_de_nuevo(muni: str) -> Dict[str, Any]:
    return {"inline_keyboard": [[{"text": "🔄 Consultar de nuevo", "callback_data": f"consultar:{muni}"}]]}

def inline_only_corregir() -> Dict[str, Any]:
    return {"inline_keyboard": [[{"text": "🧹 Corregir municipio", "callback_data": "invalid_reset"}]]}

async def send_message(chat_id: int, text: str,
                       parse_mode: Optional[str] = "Markdown",
                       reply_markup: Optional[Dict[str, Any]] = None):
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode: payload["parse_mode"] = parse_mode
    if reply_markup: payload["reply_markup"] = reply_markup
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            await client.post(f"{API_URL}/sendMessage", json=payload)
    except Exception as e:
        print(f"[send_message] {e}")

# ========= Webhook =========
@app.post("/webhook")
async def telegram_webhook(request: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)):

    if WEBHOOK_SECRET and x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
        return JSONResponse({"ok": True})

    update = await request.json()
    message = update.get("message") or {}
    text = message.get("text", "")
    chat = message.get("chat") or {}
    chat_id = chat.get("id")

    if not chat_id or not text:
        return {"ok": True}

    if text.lower().startswith("municipio"):
        nombre = text.split(" ", 1)[1] if " " in text else ""
        exacto, sugerido = validar_municipio(nombre)

        if not exacto and not sugerido:
            await send_message(chat_id,
                f"⚠️ No encontré *{nombre}* en la lista oficial de municipios.\n\n"
                "Verifica la ortografía o corrígelo.",
                reply_markup=inline_only_corregir())
            return {"ok": True}

        if sugerido and not exacto:
            await send_message(chat_id,
                f"⚠️ No encontré *{nombre}* en la lista oficial.\n\n"
                f"¿Quisiste decir *{sugerido}*?\n\n"
                "Si fue un error, corrígelo:",
                reply_markup=inline_only_corregir())
            return {"ok": True}

        oficial = exacto or sugerido
        counts = await get_counts_cached()
        n = 0
        for k, v in counts.items():
            if normalize(k) == normalize(oficial):
                n = v
                break

        set_user_municipio(str(chat_id), oficial)
        await send_message(chat_id,
            f"✅ Registré *{oficial}* para este chat.\n\n"
            f"Actualmente lleva {n} registro(s).",
            reply_markup=inline_consultar_de_nuevo(oficial))
        return {"ok": True}

    return {"ok": True}
