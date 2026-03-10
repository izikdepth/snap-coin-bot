import discord
import os
import sqlite3
from dotenv import load_dotenv
from discord.ext import commands
from discord import app_commands

load_dotenv("assets.env")
             
class Reward(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
    
    @app_commands.command(name="reward", description="check total rewards for a user")
    async def reward(self, interaction: discord.Interaction, user: discord.User):
        # Defer the response to avoid timeout 
        await interaction.response.defer()

        db = os.getenv("REWARDS_DB", "rewards.db") 
        conn = None 
        
        try:
            conn = sqlite3.connect(db)
            cursor = conn.cursor()
            
            cursor.execute('''SELECT SUM(reward_amount) FROM rewards WHERE user_id = ?''', (user.id,))
            total = cursor.fetchone()[0]
            await interaction.followup.send(f"{user.display_name} has {total or 0} rewards.")
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            await interaction.followup.send("Sorry, there was an error checking rewards.", ephemeral=True)
        finally:
            if conn:
                conn.close()
                
    @app_commands.command(name="add_wallet", description="add your snap wallet address to receive rewards")
    async def add_wallet(self, interaction: discord.Interaction, wallet_address: str):
        await interaction.response.defer(ephemeral=True)
        
        if not wallet_address.isalnum() or len(wallet_address) !=50:
            await interaction.followup.send("Invalid wallet address. Please provide a valid snap coin address.", ephemeral=True)
            return
        
        db = os.getenv("REWARDS_DB", "rewards.db")
        conn = sqlite3.connect(db)
        cursor = conn.cursor()
        
        cursor.execute('''INSERT OR REPLACE INTO addresses (user_id, wallet_address) VALUES (?, ?)''', (interaction.user.id, wallet_address))
        conn.commit()
        conn.close()
        
        await interaction.followup.send(f"{interaction.user.display_name}, your wallet address {wallet_address} has been added.", ephemeral=True)

async def setup(bot):
    await bot.add_cog(Reward(bot))