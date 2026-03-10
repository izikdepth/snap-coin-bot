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
# Maps user_id -> timestamp of their last message (within current period)
user_last_message: dict[int, datetime] = {}

logger = logging.getLogger('snap.lottery')


def record_user_message(user_id: int):
    user_last_message[user_id] = datetime.now(timezone.utc)
      
def update_last_rewarded(user_id: int):
    last_rewarded[user_id] = datetime.now(timezone.utc)   
    
def is_member_eligible_for_lottery(member: discord.Member, guild_id: int) -> bool:
    if member.bot:
        return False
    
    if member.guild.id != guild_id:
        return False
    
    if member.joined_at is None:
        return False
    
    time_in_guild = datetime.now(timezone.utc) - member.joined_at
    
    return time_in_guild >= timedelta(hours=1)



# returns members who sent atleast one message during period start and are eligible for lottery
def get_active_eligible_users(
    guild: discord.Guild,
    guild_id: int,
    period_start: datetime,
) -> list[discord.Member]:
    eligible = []
    now = datetime.now(timezone.utc)

    for user_id, last_msg_time in user_last_message.items():
        # Must have messaged within the current lottery period
        if not (period_start <= last_msg_time <= now):
            continue

        member = guild.get_member(user_id)
        if member and is_member_eligible_for_lottery(member, guild_id):
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

    logger.info(
        "Starting lottery task for guild %s, snapshot %s min, reward %s",
        guild_id, snapshot_time, lottery_reward_amount,
    )

    period_start = datetime.now(timezone.utc)

    while not bot.is_closed():
        try:
            await asyncio.sleep(snapshot_time * 60)

            guild = bot.get_guild(guild_id)
            if not guild:
                logger.warning("Guild %s not found", guild_id)
                period_start = datetime.now(timezone.utc)
                continue

            eligible_users = get_active_eligible_users(guild, guild_id, period_start)
            logger.info(
                "%d eligible user(s) this round: %s",
                len(eligible_users), [m.name for m in eligible_users],
            )

            if eligible_users:
                winner = random.choice(eligible_users)
                logger.info("Winner: %s", winner.name)

                # Check wallet before awarding
                wallet_connected = await is_user_address_connected(winner.id)
                if wallet_connected:
                    await add_reward_to_db(winner.id, "lottery", lottery_reward_amount)
                else:
                    logger.info("Winner %s has no wallet connected", winner.name)

                # Find and react to the winner's single most recent message
                emoji = os.getenv("REWARD_EMOJI", "🎉")
                winner_message_found = False
                general_channel = discord.utils.get(guild.text_channels, name="general")

                for text_channel in guild.text_channels:
                    if winner_message_found:
                        break
                    try:
                        async for message in text_channel.history(limit=100):
                            # Skip service messages
                            if message.type in (
                                discord.MessageType.new_member,
                                discord.MessageType.premium_guild_subscription,
                                discord.MessageType.premium_guild_tier_1,
                                discord.MessageType.premium_guild_tier_2,
                                discord.MessageType.premium_guild_tier_3,
                            ):
                                continue

                            if message.author.id == winner.id:
                                await message.add_reaction(emoji)
                                winner_message_found = True

                                if not wallet_connected and general_channel:
                                    await general_channel.send(
                                        f"Congratulations {winner.mention}! You won the lottery, "
                                        f"but you haven't connected your wallet yet. "
                                        f"Use /add_wallet to receive your reward."
                                    )
                                break  # stop after the first (most recent) message

                    except discord.Forbidden:
                        logger.warning("Missing permissions in #%s", text_channel.name)
                    except Exception as e:  # pylint: disable=broad-except
                        logger.warning("Error checking #%s: %s", text_channel.name, e)

                if not winner_message_found:
                    logger.warning("Could not find recent message from %s", winner.name)
            else:
                logger.info("No active eligible users this round.")

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Error in lottery task: %s", e)

        finally:
            # Reset tracking for next period regardless of success/failure
            user_last_message.clear()
            period_start = datetime.now(timezone.utc)
            logger.info("Tracking reset. Next lottery in %s minutes.", snapshot_time)

async def is_user_address_connected(user_id: int) -> bool:
    db = os.getenv("REWARDS_DB", "rewards.db")
    
    conn = None 
    try:
        conn = sqlite3.connect(db)
        cursor = conn.cursor()
        
        cursor.execute('''SELECT wallet_address FROM addresses WHERE user_id = ?''', (user_id,))
        result = cursor.fetchone()
        
        return result is not None and result[0] != ""
    except sqlite3.Error as e:
        logger.error("Database error checking wallet address: %s", e)
        return False
    finally:
        if conn:
            conn.close()
