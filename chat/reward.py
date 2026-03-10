import sqlite3
import os
from venv import logger
from dotenv import load_dotenv

load_dotenv("assets.env")

async def add_reward_to_db(user_id: int, reward_name: str, reward_amount: int):
    db = os.getenv("REWARDS_DB", "rewards.db")
    conn = None
    
    try:
        conn = sqlite3.connect(db)
        cursor = conn.cursor()
        
        cursor.execute('''INSERT INTO rewards (user_id, reward_name, reward_amount)
                          VALUES (?, ?, ?)
                          ON CONFLICT(user_id, reward_name) 
                          DO UPDATE SET reward_amount = reward_amount + ?''',
                       (user_id, reward_name, reward_amount, reward_amount))
        
        conn.commit()
        
    except sqlite3.Error as e:
        logger.error("Database error: %s", e)
    finally:
        if conn:
            conn.close()
            