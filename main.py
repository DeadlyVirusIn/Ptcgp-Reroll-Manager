import discord
from discord.ext import commands
import asyncio
import logging
import os
import datetime
import time
import sqlite3
import re
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Import configuration
import config

# Import utilities
from utils import (
    localize, format_number_to_k, format_minutes_to_days, sum_int_array, 
    sum_float_array, round_to_one_decimal, round_to_two_decimals, 
    count_digits, extract_numbers, extract_two_star_amount, is_numbers,
    convert_min_to_ms, convert_ms_to_min, split_multi, replace_last_occurrence,
    replace_miss_count, replace_miss_needed, send_received_message,
    send_channel_message, bulk_delete_messages, color_text, add_text_bar,
    format_number_with_spaces, get_random_string_from_array, get_oldest_message,
    wait, replace_any_logo_with, normalize_ocr, get_lasts_anti_cheat_messages
)

# Import upload utilities
from upload_utils import update_gist

# Import miss messages
from miss_sentences import (
    get_low_tension_message, get_medium_tension_message, get_high_tension_message,
    init_emojis, find_emoji
)

# Import GP test utilities
from enhanced_gp_test_utils import (
    add_miss, add_noshow, reset_test, get_test_summary, extract_godpack_id_from_message,
    get_db_connection
)

# Import core utilities
from core_utils import (
    get_guild, get_member_by_id, get_pack_specific_channel, get_users_stats,
    create_enhanced_stats_embed, get_enhanced_selected_packs_embed_text,
    create_timeline_stats, send_stats, send_ids, update_gp_tracking_list,
    update_inactive_gps, update_eligible_ids, mark_as_dead, set_user_state,
    update_server_data, update_anti_cheat, update_user_data_gp_live,
    add_user_data_gp_live, extract_gp_info, extract_double_star_info,
    create_forum_post, send_status_header, inactivity_check, create_leaderboards,
    check_file_exists_or_create, set_user_attrib_value, get_user_attrib_value,
    get_active_users, get_all_users, get_active_ids
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(filename='bot.log', encoding='utf-8', mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("bot")

# Initialize data directory and files
os.makedirs('data', exist_ok=True)

# Initialize data files
check_file_exists_or_create(os.path.join('data', 'UserData.xml'), "Users")
check_file_exists_or_create(os.path.join('data', 'ServerData.xml'), "root")

# Initialize database
db_path = os.path.join('data', 'gpp_test.db')
conn = sqlite3.connect(db_path)
conn.close()

# Set up intents
intents = discord.Intents.default()
intents.messages = True
intents.message_content = True
intents.guilds = True
intents.members = True

# Create bot instance
bot = commands.Bot(command_prefix='!', intents=intents)

# Global variables
start_interval_time = time.time()

async def backup_data_files():
    """Back up the data files to prevent data loss."""
    try:
        # Get current timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Back up UserData.xml
        if os.path.exists('data/UserData.xml'):
            with open('data/UserData.xml', 'r', encoding='utf-8') as src:
                with open(f'data/UserData_{timestamp}.xml.bak', 'w', encoding='utf-8') as dst:
                    dst.write(src.read())
                    
        # Back up ServerData.xml
        if os.path.exists('data/ServerData.xml'):
            with open('data/ServerData.xml', 'r', encoding='utf-8') as src:
                with open(f'data/ServerData_{timestamp}.xml.bak', 'w', encoding='utf-8') as dst:
                    dst.write(src.read())
                    
        logger.info(f"Data files backed up with timestamp {timestamp}")
    except Exception as e:
        logger.error(f"Error backing up data files: {e}")

@bot.event
async def on_ready():
    """Handle bot startup and initialize scheduled tasks."""
    logger.info(f'Bot logged in as {bot.user}')
    
    # Initialize scheduler
    scheduler = AsyncIOScheduler()
    
    # Setup scheduled tasks
    if config.reset_server_data_frequently:
        scheduler.add_job(update_server_data, 'interval', 
                         minutes=config.reset_server_data_time,
                         args=[bot])
    
    # Add other scheduled tasks
    scheduler.add_job(update_inactive_gps, 'interval', 
                     hours=1, args=[bot])
    
    scheduler.add_job(update_eligible_ids, 'interval', 
                     hours=6, args=[bot])
    
    scheduler.add_job(send_status_header, 'interval', 
                     hours=12, args=[bot])
    
    # Add backup job (once a day)
    scheduler.add_job(backup_data_files, 'cron', hour=3, minute=0)  # Run at 3:00 AM
    
    if config.auto_kick:
        scheduler.add_job(inactivity_check, 'interval', 
                         minutes=config.refresh_interval,
                         args=[bot])
    
    if config.anti_cheat:
        scheduler.add_job(update_anti_cheat, 'interval', 
                         minutes=config.anti_cheat_rate,
                         args=[bot])
    
    # Updates stats every X minutes
    scheduler.add_job(send_stats, 'interval', 
                     minutes=config.refresh_interval,
                     args=[bot])
    
    # Update GP tracking list
    scheduler.add_job(update_gp_tracking_list, 'interval', 
                     minutes=config.gp_tracking_update_interval,
                     args=[bot])
    
    # Start the scheduler
    scheduler.start()
    
    # Initialize emojis
    init_emojis(bot)
    
    # Initial server data update
    await update_server_data(bot, startup=True)
    
    # Initial setup for UI elements
    await send_status_header(bot)
    
    logger.info("Bot initialization complete")

@bot.command()
async def ping(ctx):
    await ctx.send("pong")

@bot.command(name="setplayerid", description="Set your player ID")
async def set_player_id(ctx, player_id: str):
    """Set user's player ID."""
    if not player_id.isdigit():
        await ctx.reply("Player ID must contain only numbers.")
        return
        
    await set_user_attrib_value(str(ctx.author.id), ctx.author.name, 'pocket_id', player_id)
    await ctx.reply(f"Your player ID has been set to: {player_id}")

@bot.command(name="setaverageinstances", description="Set your average number of instances")
async def set_average_instances(ctx, average_instances: int):
    """Set user's average instances."""
    if average_instances <= 0:
        await ctx.reply("Average instances must be greater than 0.")
        return
        
    await set_user_attrib_value(str(ctx.author.id), ctx.author.name, 'average_instances', average_instances)
    await ctx.reply(f"Your average instances has been set to: {average_instances}")

@bot.command(name="setprefix", description="Set your username prefix")
async def set_prefix(ctx, prefix: str):
    """Set user's prefix for reroll usernames."""
    await set_user_attrib_value(str(ctx.author.id), ctx.author.name, 'prefix', prefix)
    await ctx.reply(f"Your username prefix has been set to: {prefix}")

@bot.command(name="active", description="Set yourself as active")
async def active(ctx):
    """Set user as active."""
    await set_user_state(bot, ctx.author, "active", ctx)

@bot.command(name="inactive", description="Set yourself as inactive")
async def inactive(ctx):
    """Set user as inactive."""
    await set_user_state(bot, ctx.author, "inactive", ctx)

@bot.command(name="farm", description="Set yourself as a farmer (no main instance)")
async def farm(ctx):
    """Set user as farmer."""
    await set_user_state(bot, ctx.author, "farm", ctx)

@bot.command(name="leech", description="Set yourself as a leecher (only main instance)")
async def leech(ctx):
    """Set user as leecher."""
    await set_user_state(bot, ctx.author, "leech", ctx)

@bot.command(name="refresh", description="Refresh stats")
async def refresh(ctx):
    """Refresh stats display."""
    await send_stats(bot)
    await ctx.reply("Stats refreshed!")

@bot.command(name="forcerefresh", description="Force refresh the IDs list")
@commands.has_permissions(administrator=True)
async def force_refresh(ctx):
    """Force refresh the IDs list."""
    await send_ids(bot)
    await ctx.reply("IDs list refreshed!")

@bot.command(name="miss", description="Report a miss test")
async def miss_command(ctx):
    """Report a miss test for a godpack."""
    # Get thread ID and check if we're in a thread
    if not isinstance(ctx.channel, discord.Thread):
        await ctx.reply("This command can only be used in a godpack verification thread.")
        return
        
    # Get godpack ID
    godpack_id = extract_godpack_id_from_message(await get_oldest_message(ctx.channel))
    if not godpack_id:
        await ctx.reply("Could not find godpack ID in this thread.")
        return
    
    # Add miss test
    chance = await add_miss(str(ctx.guild.id), godpack_id, str(ctx.author.id))
    
    # Get miss message based on chance
    message = ""
    if chance < 33:
        message = get_high_tension_message()
    elif chance < 66:
        message = get_medium_tension_message()
    else:
        message = get_low_tension_message()
    
    await ctx.reply(message)

@bot.command(name="noshow", description="Report a test without showing the godpack")
async def noshow_command(ctx, open_slots: int, number_friends: int):
    """Report a noshow test for a godpack."""
    # Get thread ID and check if we're in a thread
    if not isinstance(ctx.channel, discord.Thread):
        await ctx.reply("This command can only be used in a godpack verification thread.")
        return
        
    # Get godpack ID
    godpack_id = extract_godpack_id_from_message(await get_oldest_message(ctx.channel))
    if not godpack_id:
        await ctx.reply("Could not find godpack ID in this thread.")
        return
    
    # Add noshow test
    chance = await add_noshow(str(ctx.guild.id), godpack_id, str(ctx.author.id), open_slots, number_friends)
    
    # Get miss message based on chance
    message = ""
    if chance < 33:
        message = get_high_tension_message()
    elif chance < 66:
        message = get_medium_tension_message()
    else:
        message = get_low_tension_message()
    
    await ctx.reply(message)

@bot.command(name="resettest", description="Reset tests for a specific godpack")
async def reset_test_command(ctx):
    """Reset tests for a godpack."""
    # Get thread ID and check if we're in a thread
    if not isinstance(ctx.channel, discord.Thread):
        await ctx.reply("This command can only be used in a godpack verification thread.")
        return
        
    # Get godpack ID
    godpack_id = extract_godpack_id_from_message(await get_oldest_message(ctx.channel))
    if not godpack_id:
        await ctx.reply("Could not find godpack ID in this thread.")
        return
    
    # Reset tests
    chance = await reset_test(str(ctx.guild.id), godpack_id, str(ctx.author.id))
    
    await ctx.reply(f"Your tests for this godpack have been reset. Current probability: {chance:.1f}%")

@bot.command(name="testsummary", description="Display a summary of tests for a godpack")
async def test_summary_command(ctx):
    """Display a summary of tests for a godpack."""
    # Get thread ID and check if we're in a thread
    if not isinstance(ctx.channel, discord.Thread):
        await ctx.reply("This command can only be used in a godpack verification thread.")
        return
        
    # Get godpack ID
    godpack_id = extract_godpack_id_from_message(await get_oldest_message(ctx.channel))
    if not godpack_id:
        await ctx.reply("Could not find godpack ID in this thread.")
        return
    
    # Get test summary
    summary = await get_test_summary(str(ctx.guild.id), godpack_id)
    
    await ctx.reply(summary)

@bot.command(name="verified", description="Mark a GP as verified")
async def verified_command(ctx):
    """Mark a godpack as verified."""
    # Get thread ID and check if we're in a thread
    if not isinstance(ctx.channel, discord.Thread):
        await ctx.reply("This command can only be used in a godpack verification thread.")
        return
    
    # Change thread name
    new_thread_name = replace_any_logo_with(ctx.channel.name, config.text_verified_logo)
    await ctx.channel.edit(name=new_thread_name)
    
    text_verified = localize("Godpack marqué comme vérifié!", "Godpack marked as verified!")
    await ctx.reply(f"{config.text_verified_logo} {text_verified}")
    
    # Update eligible IDs and GP tracking
    await update_eligible_ids(bot)
    await update_gp_tracking_list(bot)

@bot.command(name="dead", description="Mark a GP as dead/invalid")
async def dead_command(ctx):
    """Mark a godpack as dead."""
    await mark_as_dead(bot, ctx)

@bot.command(name="liked", description="Mark a GP as liked")
async def liked_command(ctx):
    """Mark a godpack as liked."""
    # Get thread ID and check if we're in a thread
    if not isinstance(ctx.channel, discord.Thread):
        await ctx.reply("This command can only be used in a godpack verification thread.")
        return
    
    # Change thread name
    new_thread_name = replace_any_logo_with(ctx.channel.name, config.text_liked_logo)
    await ctx.channel.edit(name=new_thread_name)
    
    text_liked = localize("Godpack marqué comme aimé!", "Godpack marked as liked!")
    await ctx.reply(f"{config.text_liked_logo} {text_liked}")
    
    # Update eligible IDs and GP tracking
    await update_eligible_ids(bot)
    await update_gp_tracking_list(bot)

@bot.command(name="notliked", description="Mark a GP as not liked")
async def not_liked_command(ctx):
    """Mark a godpack as not liked."""
    # Get thread ID and check if we're in a thread
    if not isinstance(ctx.channel, discord.Thread):
        await ctx.reply("This command can only be used in a godpack verification thread.")
        return
    
    # Change thread name
    new_thread_name = replace_any_logo_with(ctx.channel.name, config.text_not_liked_logo)
    await ctx.channel.edit(name=new_thread_name)
    
    text_not_liked = localize("Godpack marqué comme pas aimé!", "Godpack marked as not liked!")
    await ctx.reply(f"{config.text_not_liked_logo} {text_not_liked}")
    
    # Update eligible IDs and GP tracking
    await update_eligible_ids(bot)
    await update_gp_tracking_list(bot)

@bot.command(name="timelinestats", description="Display activity statistics over time")
async def timeline_stats_command(ctx, days: int = 7):
    """Display timeline statistics."""
    # Limit days to reasonable range
    days = max(1, min(days, 30))
    
    # Generate timeline stats
    timeline_embed = await create_timeline_stats(bot, days)
    
    await ctx.reply(embed=timeline_embed)

@bot.command(name="refreshgplist", description="Refresh the GP tracking list")
async def refresh_gp_list_command(ctx):
    """Refresh the GP tracking list."""
    await update_gp_tracking_list(bot)
    await ctx.reply("GP tracking list refreshed!")

@bot.command(name="addgpfound", description="Add a GP to a user's stats (admin only)")
@commands.has_permissions(administrator=True)
async def add_gp_found_command(ctx, member: discord.Member):
    """Add a godpack to a user's found count."""
    gp_count = int(await get_user_attrib_value(str(member.id), 'god_pack_found', 0))
    await set_user_attrib_value(str(member.id), member.name, 'god_pack_found', gp_count + 1)
    
    await ctx.reply(f"Added 1 godpack to {member.mention}'s stats. New total: {gp_count + 1}")

@bot.command(name="removegpfound", description="Remove a GP from a user's stats (admin only)")
@commands.has_permissions(administrator=True)
async def remove_gp_found_command(ctx, member: discord.Member):
    """Remove a godpack from a user's found count."""
    gp_count = int(await get_user_attrib_value(str(member.id), 'god_pack_found', 0))
    if gp_count > 0:
        await set_user_attrib_value(str(member.id), member.name, 'god_pack_found', gp_count - 1)
        await ctx.reply(f"Removed 1 godpack from {member.mention}'s stats. New total: {gp_count - 1}")
    else:
        await ctx.reply(f"{member.mention} has no godpacks to remove.")

# Button interaction handler
@bot.event
async def on_interaction(interaction):
    if interaction.type == discord.InteractionType.component:
        custom_id = interaction.data.get("custom_id", "")
        
        if custom_id in ["active", "inactive", "farm", "leech"]:
            await interaction.response.defer(ephemeral=True)
            await set_user_state(bot, interaction.user, custom_id, interaction)
        elif custom_id == "refreshUserStats":
            await interaction.response.defer(ephemeral=True)
            await send_stats(bot)
            await interaction.followup.send("Stats refreshed!", ephemeral=True)

# Message handler for godpack detection
@bot.event
async def on_message(message):
    # Process commands first
    await bot.process_commands(message)
    
    # Skip if not in webhook channel
    if message.channel.id != int(config.channel_id_webhook):
        return
    
    # Skip if not from webhook or doesn't have attachments
    if not message.webhook_id or not message.attachments:
        return
    
    # Check if it's a godpack message
    if "godpack" in message.content.lower():
        gp_info = extract_gp_info(message)
        channel_id = await get_pack_specific_channel(gp_info["pack_booster_type"])
        await create_forum_post(
            bot, message, channel_id, "GodPack", 
            f"{gp_info['account_name']} [{gp_info['pack_amount']}P][{gp_info['two_star_ratio']}/5]", 
            gp_info["owner_id"], gp_info["account_id"], 
            gp_info["pack_amount"], gp_info["pack_booster_type"]
        )
    
    # Handle double star
    elif "double star" in message.content.lower():
        ds_info = extract_double_star_info(message)
        await create_forum_post(
            bot, message, config.channel_id_2star_verification_forum,
            "DoubleStar", ds_info["account_name"], 
            ds_info["owner_id"], ds_info["account_id"], 
            ds_info["pack_amount"]
        )

# Error handler
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.reply("You don't have permission to use this command.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply(f"Missing required argument: {error.param.name}")
    elif isinstance(error, commands.BadArgument):
        await ctx.reply(f"Invalid argument provided: {error}")
    else:
        logger.error(f"Command error: {error}")
        await ctx.reply(f"An error occurred: {error}")

# Run the bot
if __name__ == "__main__":
    bot.run(config.token)