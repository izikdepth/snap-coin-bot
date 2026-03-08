import asyncio
import sqlite3
import discord
from chat.reward import add_reward_to_db
import os
import random
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import logging

load_dotenv("assets.env")

active_users: set[int] = set()
last_rewarded = {}

logger = logging.getLogger('snap.lottery')

    
def update_last_rewarded(user_id: int):
    last_rewarded[user_id] = datetime.now(timezone.utc)   
    
def is_member_eligible_for_lottery(member: discord.Member, message: discord.Message, guild_id: int) -> bool:
    if member.bot:
        return False
    
    if member.guild.id != guild_id:
        return False
    
    if member.joined_at is None:
        return False
    
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
            return False
    

    time_in_guild = datetime.now(timezone.utc) - member.joined_at
    
    return time_in_guild >= timedelta(hours=1)


# track a user during this set lottery period
def track_active_user(user_id: int):
    snapshot_time = int(os.getenv("SNAPSHOT_LOTTERY_TIME", "60"))
    
    last_time = last_rewarded.get(user_id)
    now = datetime.now(timezone.utc)
    
    # Only track if user hasn't been rewarded in this interval
    if last_time is None or (now - last_time) >= snapshot_time:
        active_users.add(user_id)
    

def get_active_eligible_users(guild: discord.Guild, message: discord.Message,  guild_id: int) -> list[discord.Member]:
    eligible = []
    
    for user_id in active_users:
        member = guild.get_member(user_id)
        if member and is_member_eligible_for_lottery(member, message, guild_id):
            eligible.append(member)
    
    return eligible

async def lottery_task(bot):
    await bot.wait_until_ready()
    
    try:
        guild_id = int(os.getenv("GUILD_ID", "0"))
        snapshot_time = int(os.getenv("SNAPSHOT_LOTTERY_TIME", "60"))
        lottery_reward_amount = float(os.getenv("LOTTERY_REWARD_AMOUNT", "0.1"))
    except ValueError as e:
        logger.error("Invalid environment variable: %s", e)
        return
    
    logger.info("Starting lottery task for guild %s with snapshot time %s minutes and reward amount %s", guild_id, snapshot_time, lottery_reward_amount)
    
    while not bot.is_closed():
        try:
            guild = bot.get_guild(guild_id)
                
            if not guild:
                logger.warning("Guild %s not found", guild_id)
                await asyncio.sleep(60)
                continue
        
            # get eligible users for this period (message=None because we're tracking joined_at for eligibility)
            eligible_users = get_active_eligible_users(guild, None,  guild_id)
            
            if eligible_users:
                winner = random.choice(eligible_users)
                update_last_rewarded(winner.id)
                
                # Add reward to database
                await add_reward_to_db(winner.id, "lottery", lottery_reward_amount)
                
                # react to the winner's most recent message
                emoji = os.getenv("REWARD_EMOJI", "🎉")
                winner_message_found = False
                
                general_channel = discord.utils.get(guild.text_channels, name="general")
                
                for text_channel in guild.text_channels:
                    try:
                        async for message in text_channel.history(limit=50):
                            if message.author.id == winner.id:
                                await message.add_reaction(emoji)
                                winner_message_found = True
                                
                                # check if user has connected wallet, if not prompt them to connect
                                if not await is_user_address_connected(winner.id):
                                    await general_channel.send(f"Congratulations {winner.mention}! You have won the lottery, but it looks like you haven't connected your wallet yet. Please use the /add_wallet command to connect your wallet address and receive your reward.")
                            
                                break
                            
                        if winner_message_found:
                            break  
                        
                    except discord.Forbidden:
                        logger.warning(" Missing permissions in #%s", text_channel.name)
                    except Exception as e: # pylint: disable=broad-except
                        logger.warning("Error checking #%s: %s", text_channel.name, e)
                        continue
                
                if not winner_message_found:
                    logger.warning("   Could not find recent message from %s", winner.name)
            else:
                logger.info("No active eligible users for lottery this round")
                    
            # reset active users for next period
            active_users.clear()
            logger.info("Active users cleared. Next lottery in %s minutes.", snapshot_time)
            
            await asyncio.sleep(snapshot_time * 60)
            
        except Exception as e: # pylint: disable=broad-except
            logger.error("Error in lottery task: %s", e)
            await asyncio.sleep(60)


async def is_user_address_connected(user_id: int) -> bool:
    db = os.getenv("REWARDS_DB", "rewards.db")
    
    conn = None 
    try:
        conn = sqlite3.connect(db)
        cursor = conn.cursor()
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS addresses
                          (user_id INTEGER PRIMARY KEY, wallet_address TEXT)''')
        
        cursor.execute('''SELECT wallet_address FROM addresses WHERE user_id = ?''', (user_id,))
        result = cursor.fetchone()
        
        return result is not None and result[0] != ""
    except sqlite3.Error as e:
        logger.error("Database error checking wallet address: %s", e)
        return False
    finally:
        if conn:
            conn.close()
