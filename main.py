import json
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiogram.enums import ParseMode
import logging
import asyncio
from aggregate_data import aggregate_data
import os

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
bot = Bot(os.getenv("API_TOKEN"))
dp = Dispatcher()

@dp.message(CommandStart())
async def send_welcome(message: types.Message):
    logging.info("Processing message: %s", message.text)
    await message.reply("Send me a JSON with the format: {\"dt_from\":\"YYYY-MM-DDTHH:MM:SS\", \"dt_upto\":\"YYYY-MM-DDTHH:MM:SS\", \"group_type\":\"hour|day|month\"}")

@dp.message()
async def handle_message(message: types.Message):
    logging.info("Processing message: %s", message.text)
    try:
        data = json.loads(message.text)
        logging.info("\ninput: %s", data)
        dt_from = data["dt_from"]
        dt_upto = data["dt_upto"]
        group_type = data["group_type"]
        
        result = await aggregate_data(dt_from, dt_upto, group_type)
        logging.info("\noutput: %s", json.dumps(result))
        await message.reply(json.dumps(result), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await message.reply(f"Error: {e}")

async def main():
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())