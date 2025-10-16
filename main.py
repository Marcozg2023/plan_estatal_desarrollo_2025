from fastapi import FastAPI, Request
import httpx
import os

# Variables de entorno
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "missecreto")

app = FastAPI()


@app.get("/")
def home():
    return {"status": "ok", "message": "Chatbot PED activo"}


@app.post("/webhook")
async def telegram_webhook(request: Request):
    update = await request.json()

    # Verificamos si hay un mensaje de texto
    if "message" in update and "text" in update["message"]:
        chat_id = update["message"]["chat"]["id"]
        text = update["message"]["text"].lower()

        # Respuestas básicas
        if "hola" in text:
            await send_message(chat_id, "¡Hola! Soy tu asistente del Plan Estatal de Desarrollo.")
        elif "ayuda" in text:
            await send_message(chat_id, "Opciones disponibles:\n- hola\n- ayuda\n- info")
        elif "info" in text:
            await send_message(chat_id, "Este es un chatbot de prueba para la participación ciudadana en el PED.")
        else:
            await send_message(chat_id, "No entendí tu mensaje 🤔. Escribe 'ayuda' para ver opciones.")

    return {"ok": True}


async def send_message(chat_id: int, text: str):
    async with httpx.AsyncClient() as client:
        await client.post(f"{API_URL}/sendMessage", json={"chat_id": chat_id, "text": text})
