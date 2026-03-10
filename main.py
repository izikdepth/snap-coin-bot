import os
import discord
from discord.ext import commands
from dotenv import load_dotenv
from chat.lottery import lottery_task,  record_user_message
import logging
from logging.handlers import RotatingFileHandler
import asyncio
import sqlite3

from payment.pay_out import request_payout

load_dotenv(".env")

os.makedirs('logs', exist_ok=True)
handler = RotatingFileHandler('logs/bot.log', maxBytes=5*1024*1024, backupCount=5)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[handler, logging.StreamHandler()]
)
logger = logging.getLogger('snap')
logging.getLogger('discord').setLevel(logging.WARNING)

bot = commands.Bot(command_prefix="/", intents=discord.Intents.all())

def init_db():
    load_dotenv("assets.env")

    db = os.getenv("REWARDS_DB", "rewards.db")
    
    conn = None 
    try:
        conn = sqlite3.connect(db)
        cursor = conn.cursor()
        
        # Create tables only once
        cursor.execute('''CREATE TABLE IF NOT EXISTS addresses
                          (user_id INTEGER PRIMARY KEY, wallet_address TEXT)''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS rewards
                          (user_id INTEGER, 
                           reward_name TEXT, 
                           reward_amount INTEGER,
                           PRIMARY KEY (user_id, reward_name))''')
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS last_active 
                          (user_id INTEGER PRIMARY KEY, last_time TEXT)''')
        
        conn.commit()
        logger.info("Database initialized successfully.")
    
    except sqlite3.Error as e:
        logger.error("Error initializing Database: %s", e)
        return False
    
    finally:
        if conn:
            conn.close()   
            
async def set_hookup():
    asyncio.create_task(request_payout(bot))
    
@bot.event
async def setup_hook():
    await bot.load_extension("commands.chat")
    
    # start the lottery background task
    bot.loop.create_task(lottery_task(bot))
    # start the background payout task
    bot.loop.create_task(request_payout(bot))
    logger.info("Extensions loaded and lottery task scheduled")

@bot.event
async def on_ready():
    logger.info("Logged in as %s (ID: %s)", bot.user, bot.user.id)
    logger.info("Bot is ready in guild %s", os.getenv('GUILD_ID'))
    await bot.tree.sync()

@bot.event
async def on_message(message: discord.Message):
    # Ignore bot messages
    if message.author.bot:
        return
    
    # Only check message.type if message exists
    if message is not None:
        # ignore service messages eg joim ,boost, etc
        if message.type in (
            discord.MessageType.new_member,
            discord.MessageType.premium_guild_subscription,
            discord.MessageType.premium_guild_tier_1,
            discord.MessageType.premium_guild_tier_2,
            discord.MessageType.premium_guild_tier_3,
        ):
            return
    record_user_message(message.author.id)
    await bot.process_commands(message)
    # Track active users for lottery
    # try:
    #     guild_id = int(os.getenv("GUILD_ID", "0"))
    #     if message.guild and message.guild.id == guild_id:
    #         # Ignore commands
    #         if not (message.content.startswith("/") or message.content.startswith("\\") or message.content.startswith("!")):
    #             load_dotenv("assets.env")
    #             snapshot_time = os.getenv("SNAPSHOT_LOTTERY_TIME", "10")
    #             get_active_user_within_interval(message.author.id, snapshot_time)
    #             logger.debug("Tracked active user: %s", message.author.name)
    # except (ValueError, TypeError) as e:
    #     logger.error("Error tracking user: %s", e)

if __name__ == "__main__":
    init_db()
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        logger.error("DISCORD_TOKEN not found in environment variables")
    else:
        logger.info("Starting bot...")
        bot.run(token)