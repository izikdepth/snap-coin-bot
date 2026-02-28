import asyncio
import secrets
import sqlite3
import os
import string
from dotenv import load_dotenv
import logging
import aiohttp
import discord

load_dotenv("assets.env")
load_dotenv(".env")

logger = logging.getLogger('snap.payout')

async def get_pending_withdrawals():
    db = os.getenv("REWARDS_DB", "rewards.db")
    conn = None
    try:
        conn = sqlite3.connect(db)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT r.user_id, r.reward_name, r.reward_amount, a.wallet_address
            FROM rewards r
            JOIN addresses a ON r.user_id = a.user_id
            WHERE r.reward_amount > 0
            AND a.wallet_address IS NOT NULL
            AND a.wallet_address != ''
        ''')
        
        pending = cursor.fetchall()
    
        conn.close()
        return pending

    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            return []
        raise
    finally:
        if conn:
            conn.close()
    

async def request_payout(bot, interval=int(os.getenv("PAYOUT_INTERVAL_SECONDS", "86400"))):
    while True:
        pending = await get_pending_withdrawals()
        if pending:
            user_ids = [user_id for user_id, reward_name, reward_amount, wallet_address in pending]
            receivers = [
                [wallet_address, round(reward_amount, 4)]
                for user_id, reward_name, reward_amount, wallet_address in pending
            ]

            reference = generate_base36()

            secret_key = os.getenv("SECRET_KEY")
            link = os.getenv("FAUCET_LINK")

            data = {
                "secret_key": secret_key,
                "status_reference_wallet": reference,
                "receivers": receivers,
            }
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(link, json=data, headers={'Content-Type': 'application/json'}, timeout=aiohttp.ClientTimeout(total=10)) as req:
                        response_text = await req.text()

                        logger.info("Requested payout for %d users with total amount %.4f", len(receivers), sum(r[1] for r in receivers))
                        logger.info("Payout request response: %d - %s", req.status, response_text)

                        if req.status == 200:
                            await reset_balance(user_ids)
                            
                            # wait for 10 mins, fetch the transaction hash and post the transaction url in 
                            # the general chat informing winners that payments have been made
                            
                            asyncio.sleep(10 * 60) # sleep for 10 mins
                            guild_id = int(os.getenv("GUILD_ID"))
                            guild = bot.get_guild(guild_id)
    
                            payout_proof = get_payout_proof(reference)
                            if guild:
                                post_channel = discord.utils.get(guild.text_channels, name="general")
                                
                                if post_channel and payout_proof:
                                    user_mentions = " ".join(f"<@{user_id}>" for user_id in user_ids)
                                    
                                    message = f"{user_mentions}\nYour payout for the day has been made based on how many time your messages were selected as winning messages by the bot! Transaction: {payout_proof}"
                                    await post_channel.send(message)
                            else:
                                logger.error("Guild not found or bot isn't ready yet.")
                            

            except Exception as e: #pylint: disable=broad-except
                logger.error("Payout request failed: %s", e)
        else:
            logger.info("No pending withdrawals found")

        await asyncio.sleep(interval)
        
def generate_base36(length=30):
    chars = string.digits + string.ascii_lowercase
    return ''.join(secrets.choice(chars) for _ in range(length))

async def get_payout_proof(reference):
    url = f"{os.getenv('FAUCET_LINK')}/get-withdrawals/{reference}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    # get a list of transaction hashes and create an explorer link with each hash
                    transactions = await resp.json() 
                    return [
                        f"https://explorer.snap-coin.net/tx/{tx['transaction_hash']}" 
                        for tx in transactions
                    ]
    except Exception as e: #pylint: disable=broad-except
        print(f"Error: {e}")
        return []
    return []   
    
async def reset_balance(user_ids: list):
    db = os.getenv("REWARDS_DB", "rewards.db")
    conn = None
    try:
        conn = sqlite3.connect(db)
        cursor = conn.cursor()

        cursor.executemany('''
            UPDATE rewards
            SET reward_amount = 0
            WHERE user_id = ?
        ''', [(uid,) for uid in user_ids])

        conn.commit()
    finally:
        if conn:
            conn.close()