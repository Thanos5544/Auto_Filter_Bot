import plugins.monkey_patch
from pyrogram import idle, __version__
from pyrogram.raw.all import layer
import time
from pyrogram.errors import FloodWait
import asyncio
from datetime import date, datetime
from pathlib import Path
import pytz
from database.ia_filterdb import Media, Media2, Media3, Media4
from database.users_chats_db import db
from info import MULTIPLE_DB, DATABASE_URI3, DATABASE_URI4, ON_HEROKU, LOG_STR, LOG_CHANNEL, PORT
from utils import temp
from Script import script
from plugins import web_server, check_expired_premium, keep_alive
from dreamxbotz.Bot import dreamxbotz
from dreamxbotz.util.keepalive import ping_server
from dreamxbotz.Bot.clients import initialize_clients
from PIL import Image
Image.MAX_IMAGE_PIXELS = 500_000_000

import logging
import logging.config

logging.config.fileConfig('logging.conf')
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("imdbpy").setLevel(logging.ERROR)
logging.getLogger("aiohttp").setLevel(logging.ERROR)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)
logging.getLogger("pymongo").setLevel(logging.WARNING)

botStartTime = time.time()


def get_plugins_names(plugins_dir="plugins"):
    plugins_path = Path(plugins_dir)
    if not plugins_path.exists():
        logging.warning("Plugins directory not found: %s", plugins_path)
        return []

    return [
        ".".join(file.relative_to(plugins_path).with_suffix("").parts)
        for file in sorted(plugins_path.rglob("*.py"))
        if file.name != "__init__.py"
    ]

async def dreamxbotz_start():
    logging.info('\n\nInitializing DreamxBotz')
    await dreamxbotz.start()
    bot_info = await dreamxbotz.get_me()
    dreamxbotz.username = bot_info.username
    await initialize_clients()
    plugins_names = get_plugins_names()
    if plugins_names:
        logging.info("Plugins Found (%d): %s", len(plugins_names), ", ".join(plugins_names))
    else:
        logging.warning("No plugins found.")

    if ON_HEROKU:
        asyncio.create_task(ping_server())
    b_users, b_chats = await db.get_banned()
    temp.BANNED_USERS = b_users
    temp.BANNED_CHATS = b_chats
        await Media.ensure_indexes()
        if MULTIPLE_DB:
        await Media2.ensure_indexes()
        await Media3.ensure_indexes()
        await Media4.ensure_indexes()
        if DATABASE_URI4:
            logging.info("Multiple Database Mode On. Now Files Will Be Save In 2nd, 3rd & 4th DB If 1st DB Is Full")
        elif DATABASE_URI3:
            logging.info("Multiple Database Mode On. Now Files Will Be Save In 2nd & 3rd DB If 1st DB Is Full")
        else:
            logging.info("Multiple Database Mode On. Now Files Will Be Save In Second DB If First DB Is Full")
    else:
        logging.info("Single DB Mode On ! Files Will Be Save In First Database")
    
    me = bot_info
    temp.ME = me.id
    temp.U_NAME = me.username
    temp.B_NAME = me.first_name
    temp.B_LINK = me.mention
    dreamxbotz.username = '@' + me.username
    dreamxbotz.loop.create_task(check_expired_premium(dreamxbotz))
    
    logging.info(f"{me.first_name} with Pyrogram v{__version__} (Layer {layer}) started on {me.username}.")
    logging.info(LOG_STR)
    logging.info(script.LOGO)
    tz = pytz.timezone('Asia/Kolkata')
    today = date.today()
    now = datetime.now(tz)
    current_time = now.strftime("%H:%M:%S %p")
    await dreamxbotz.send_message(chat_id=LOG_CHANNEL, text=script.RESTART_TXT.format(temp.B_LINK, today, current_time))
    app = web.AppRunner(await web_server())
    await app.setup()
    bind_address = "0.0.0.0"
    await web.TCPSite(app, bind_address, PORT).start()
    dreamxbotz.loop.create_task(keep_alive())

    try:
        await idle()
    finally:
        await app.cleanup()

if __name__ == '__main__':
    try:
        dreamxbotz.run(dreamxbotz_start())
    except FloodWait as e:
        logging.info(f"FloodWait! Sleeping for {e.value} seconds.")
        time.sleep(e.value)
    except KeyboardInterrupt:
        logging.info('Service stopped. Bye.')
