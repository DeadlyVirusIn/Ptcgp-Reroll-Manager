import discord
from discord.ext import commands, tasks
import asyncio
import datetime
import os
import sys
import traceback
from typing import Optional, List

# Import configuration and utilities
import config
from core_utils import (
    # Core functions
    get_guild, get_member_by_id, get_active_users, get_all_users,
    send_enhanced_stats, create_detailed_user_stats, create_timeline_stats_with_visualization,
    create_user_activity_chart, generate_server_report, health_check,
    
    # User management
    set_user_pack_preference, get_user_pack_preferences, does_user_profile_exists,
    set_user_attrib_value, get_user_attrib_value, cleanup_inactive_users,
    
    # Data management
    backup_user_data, restore_user_data, validate_user_data_integrity,
    log_user_activity, emergency_shutdown,
    
    # Legacy compatibility
    send_stats_legacy, get_channel_by_id
)

from utils import (
    format_number_to_k, format_minutes_to_days, round_to_one_decimal,
    send_channel_message, bulk_delete_messages
)

from miss_sentences import find_emoji

# Bot setup
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True

bot = commands.Bot(
    command_prefix=config.command_prefix,
    intents=intents,
    help_command=None,  # Disable default help command
    case_insensitive=True
)

# Global variables for tracking
stats_task_running = False
last_stats_time = None

@bot.event
async def on_ready():
    """Bot startup event."""
    print(f'✅ {bot.user.name} has connected to Discord!')
    print(f'🔗 Bot ID: {bot.user.id}')
    print(f'🌐 Connected to {len(bot.guilds)} guild(s)')
    
    # Start background tasks
    if not stats_sender.is_running():
        stats_sender.start()
        print('📊 Stats sender task started')
    
    if not user_cleanup.is_running():
        user_cleanup.start()
        print('🧹 User cleanup task started')
    
    # Perform health check on startup
    health_status = await health_check()
    print(f'🏥 Health check: {sum(health_status.values())}/{len(health_status)} systems OK')

@bot.event
async def on_command_error(ctx, error):
    """Global error handler for commands."""
    if isinstance(error, commands.CommandNotFound):
        await ctx.reply("❌ Command not found. Use `!help` to see available commands.")
    elif isinstance(error, commands.MissingPermissions):
        await ctx.reply("❌ You don't have permission to use this command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.reply("❌ Invalid argument provided. Please check the command syntax.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply(f"❌ Missing required argument: {error.param}")
    else:
        print(f"❌ Unexpected error in command {ctx.command}: {error}")
        traceback.print_exception(type(error), error, error.__traceback__)
        await ctx.reply("❌ An unexpected error occurred. Please try again.")

# BACKGROUND TASKS
@tasks.loop(minutes=config.stats_interval_minutes)
async def stats_sender():
    """Background task to send statistics periodically."""
    global stats_task_running, last_stats_time
    
    if stats_task_running:
        print("⚠️ Stats task already running, skipping...")
        return
    
    try:
        stats_task_running = True
        await send_enhanced_stats(bot, manual_trigger=False)
        last_stats_time = datetime.datetime.now()
        print(f"✅ Automated stats sent at {last_stats_time.strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"❌ Error in stats sender task: {e}")
    finally:
        stats_task_running = False

@tasks.loop(hours=24)  # Run daily
async def user_cleanup():
    """Background task to clean up inactive users."""
    try:
        await cleanup_inactive_users()
        print("✅ Daily user cleanup completed")
    except Exception as e:
        print(f"❌ Error in user cleanup task: {e}")

@tasks.loop(hours=6)  # Run every 6 hours
async def data_backup():
    """Background task to backup user data."""
    try:
        success = await backup_user_data()
        if success:
            print("✅ Automated data backup completed")
    except Exception as e:
        print(f"❌ Error in data backup task: {e}")

# BASIC COMMANDS
@bot.command(name="help", description="Show available commands")
async def help_command(ctx):
    """Display help information."""
    embed = discord.Embed(
        title="🤖 Bot Commands",
        description="Available commands for the reroll management bot",
        color=0x3498db
    )
    
    # Basic commands
    embed.add_field(
        name="📊 Statistics Commands",
        value="`!stats` - Show current statistics\n"
              "`!mystats` - Show your detailed statistics\n"
              "`!timeline [days]` - Show activity timeline\n"
              "`!report [days]` - Generate server report",
        inline=False
    )
    
    # User commands
    embed.add_field(
        name="👤 User Commands",
        value="`!setpack <pack_name>` - Set preferred pack\n"
              "`!mypack` - Show your pack preferences\n"
              "`!activity [user]` - Show user activity chart",
        inline=False
    )
    
    # Admin commands (only show if user has permissions)
    if ctx.author.guild_permissions.administrator or any(role.id in config.admin_role_ids for role in ctx.author.roles):
        embed.add_field(
            name="🔧 Admin Commands",
            value="`!forcestats` - Force send statistics\n"
                  "`!cleanup` - Clean up inactive users\n"
                  "`!backup` - Create data backup\n"
                  "`!health` - System health check\n"
                  "`!emergency` - Emergency shutdown",
            inline=False
        )
    
    embed.set_footer(text=f"Prefix: {config.command_prefix} | Use {config.command_prefix}command for detailed help")
    await ctx.reply(embed=embed)

@bot.command(name="stats", description="Display current server statistics")
async def stats_command(ctx):
    """Manual stats command."""
    try:
        await send_enhanced_stats(bot, manual_trigger=True)
        await ctx.reply("✅ Statistics sent!")
    except Exception as e:
        print(f"❌ Error in stats command: {e}")
        await ctx.reply("❌ Failed to send statistics.")

@bot.command(name="mystats", description="Show your detailed statistics")
async def my_stats_command(ctx):
    """Show detailed statistics for the command user."""
    try:
        user_embed = await create_detailed_user_stats(bot, str(ctx.author.id))
        if user_embed:
            await ctx.reply(embed=user_embed)
        else:
            await ctx.reply("❌ No statistics found for your account. Start rerolling to generate data!")
    except Exception as e:
        print(f"❌ Error in mystats command: {e}")
        await ctx.reply("❌ Failed to retrieve your statistics.")

@bot.command(name="timeline", aliases=["timelinestats"], description="Display activity timeline")
async def timeline_stats_command(ctx, days: int = 7):
    """Display enhanced timeline statistics."""
    # Limit days to reasonable range
    days = max(1, min(days, 30))
    
    try:
        timeline_embed = await create_timeline_stats_with_visualization(bot, days)
        await ctx.reply(embed=timeline_embed)
    except Exception as e:
        print(f"❌ Error generating timeline stats: {e}")
        await ctx.reply("❌ Failed to generate timeline statistics.")

@bot.command(name="report", description="Generate comprehensive server report")
async def server_report_command(ctx, days: int = 30):
    """Generate a comprehensive server activity report."""
    # Limit days to reasonable range
    days = max(1, min(days, 90))
    
    try:
        report_embed = await generate_server_report(bot, days)
        if report_embed:
            await ctx.reply(embed=report_embed)
        else:
            await ctx.reply("❌ Failed to generate server report.")
    except Exception as e:
        print(f"❌ Error generating server report: {e}")
        await ctx.reply("❌ Failed to generate server report.")

# PACK PREFERENCE COMMANDS
@bot.command(name="setpack", description="Set your preferred pack for filtering")
async def set_pack_command(ctx, *, pack_name: str = None):
    """Set user's preferred pack."""
    if not config.enable_role_based_filters:
        await ctx.reply("❌ Pack filtering is not enabled on this server.")
        return
    
    if not pack_name:
        # Show available packs
        available_packs = list(config.pack_filters.keys())
        pack_list = '\n'.join([f"• {pack}" for pack in available_packs])
        
        embed = discord.Embed(
            title="📦 Available Packs",
            description=f"Use `!setpack <pack_name>` to set your preference.\n\n**Available packs:**\n{pack_list}\n\nUse `!setpack none` to disable filtering.",
            color=0x9c59d1
        )
        await ctx.reply(embed=embed)
        return
    
    # Handle disabling pack preference
    if pack_name.lower() in ['none', 'disable', 'off']:
        success = await set_user_pack_preference(str(ctx.author.id), ctx.author.display_name, '')
        if success:
            await ctx.reply("✅ Pack filtering disabled. You'll now see all pack types.")
        else:
            await ctx.reply("❌ Failed to update pack preference.")
        return
    
    # Set specific pack preference
    if pack_name not in config.pack_filters:
        await ctx.reply(f"❌ Pack '{pack_name}' not found. Use `!setpack` to see available packs.")
        return
    
    success = await set_user_pack_preference(str(ctx.author.id), ctx.author.display_name, pack_name)
    if success:
        await ctx.reply(f"✅ Pack preference set to **{pack_name}**. Your rerolling will now focus on this pack type.")
        await log_user_activity(str(ctx.author.id), ctx.author.display_name, "pack_preference_changed", pack_name)
    else:
        await ctx.reply("❌ Failed to update pack preference.")

@bot.command(name="mypack", aliases=["packinfo"], description="Show your pack preferences")
async def my_pack_command(ctx):
    """Show user's pack preferences and statistics."""
    try:
        pack_prefs = await get_user_pack_preferences(str(ctx.author.id))
        
        embed = discord.Embed(
            title=f"📦 Pack Preferences - {ctx.author.display_name}",
            color=0x9c59d1
        )
        
        if not config.enable_role_based_filters:
            embed.description = "Pack filtering is not enabled on this server."
            await ctx.reply(embed=embed)
            return
        
        selected_pack = pack_prefs.get('selected_pack', '')
        filter_enabled = pack_prefs.get('filter_enabled', True)
        
        if selected_pack:
            embed.add_field(
                name="🎯 Current Preference",
                value=f"**{selected_pack}**",
                inline=True
            )
        else:
            embed.add_field(
                name="🎯 Current Preference",
                value="None (all packs)",
                inline=True
            )
        
        embed.add_field(
            name="🔄 Filter Status",
            value="✅ Enabled" if filter_enabled else "❌ Disabled",
            inline=True
        )
        
        # Show pack statistics
        pack_stats = pack_prefs.get('pack_statistics', {})
        if pack_stats:
            stats_text = ""
            for pack_name, count in pack_stats.items():
                if count > 0:
                    stats_text += f"**{pack_name}:** {format_number_to_k(count)} packs\n"
            
            if stats_text:
                embed.add_field(
                    name="📊 Pack Statistics",
                    value=stats_text,
                    inline=False
                )
        
        embed.set_footer(text="Use !setpack <pack_name> to change your preference")
        await ctx.reply(embed=embed)
        
    except Exception as e:
        print(f"❌ Error in mypack command: {e}")
        await ctx.reply("❌ Failed to retrieve pack preferences.")

@bot.command(name="activity", description="Show user activity chart")
async def activity_chart_command(ctx, user: discord.Member = None, days: int = 7):
    """Generate an activity chart for a user."""
    target_user = user or ctx.author
    days = max(1, min(days, 30))  # Limit to 1-30 days
    
    try:
        chart_buffer = await create_user_activity_chart(str(target_user.id), days)
        
        if chart_buffer:
            file = discord.File(chart_buffer, filename=f"activity_{target_user.display_name}_{days}d.png")
            embed = discord.Embed(
                title=f"📈 Activity Chart - {target_user.display_name}",
                description=f"Activity over the last {days} days",
                color=0x3498db
            )
            embed.set_image(url=f"attachment://activity_{target_user.display_name}_{days}d.png")
            await ctx.reply(file=file, embed=embed)
        else:
            await ctx.reply("❌ Failed to generate activity chart. User may not have enough data.")
            
    except Exception as e:
        print(f"❌ Error generating activity chart: {e}")
        await ctx.reply("❌ Failed to generate activity chart.")

# ADMIN COMMANDS
@bot.command(name="forcestats", description="Force send statistics (Admin only)")
@commands.has_permissions(administrator=True)
async def force_stats_command(ctx):
    """Force send statistics manually."""
    try:
        await send_enhanced_stats(bot, manual_trigger=True)
        await ctx.reply("✅ Statistics forcefully sent!")
        await log_user_activity(str(ctx.author.id), ctx.author.display_name, "force_stats")
    except Exception as e:
        print(f"❌ Error in forcestats command: {e}")
        await ctx.reply("❌ Failed to send statistics.")

@bot.command(name="cleanup", description="Clean up inactive users (Admin only)")
@commands.has_permissions(administrator=True)
async def cleanup_command(ctx):
    """Manually trigger user cleanup."""
    try:
        await cleanup_inactive_users()
        await ctx.reply("✅ Inactive user cleanup completed!")
        await log_user_activity(str(ctx.author.id), ctx.author.display_name, "manual_cleanup")
    except Exception as e:
        print(f"❌ Error in cleanup command: {e}")
        await ctx.reply("❌ Failed to clean up inactive users.")

@bot.command(name="backup", description="Create data backup (Admin only)")
@commands.has_permissions(administrator=True)
async def backup_command(ctx):
    """Manually trigger data backup."""
    try:
        success = await backup_user_data()
        if success:
            await ctx.reply("✅ Data backup created successfully!")
            await log_user_activity(str(ctx.author.id), ctx.author.display_name, "manual_backup")
        else:
            await ctx.reply("❌ Failed to create data backup.")
    except Exception as e:
        print(f"❌ Error in backup command: {e}")
        await ctx.reply("❌ Failed to create data backup.")

@bot.command(name="health", description="System health check (Admin only)")
@commands.has_permissions(administrator=True)
async def health_command(ctx):
    """Perform system health check."""
    try:
        health_status = await health_check()
        
        embed = discord.Embed(
            title="🏥 System Health Check",
            color=0x2ecc71 if all(health_status.values()) else 0xe74c3c
        )
        
        for component, status in health_status.items():
            status_emoji = "✅" if status else "❌"
            component_name = component.replace('_', ' ').title()
            embed.add_field(
                name=f"{status_emoji} {component_name}",
                value="OK" if status else "FAILED",
                inline=True
            )
        
        overall_status = sum(health_status.values())
        total_components = len(health_status)
        
        embed.set_footer(text=f"Overall Status: {overall_status}/{total_components} components healthy")
        
        await ctx.reply(embed=embed)
        
    except Exception as e:
        print(f"❌ Error in health command: {e}")
        await ctx.reply("❌ Failed to perform health check.")

@bot.command(name="emergency", description="Emergency shutdown (Admin only)")
@commands.has_permissions(administrator=True)
async def emergency_command(ctx, *, reason: str = "Manual emergency shutdown"):
    """Trigger emergency shutdown procedures."""
    try:
        # Confirm with user
        confirm_embed = discord.Embed(
            title="🚨 Emergency Shutdown Confirmation",
            description=f"**Reason:** {reason}\n\nThis will:\n• Backup all data\n• Set all users to inactive\n• Save emergency IDs file\n\nReact with ✅ to confirm or ❌ to cancel.",
            color=0xe74c3c
        )
        
        message = await ctx.reply(embed=confirm_embed)
        await message.add_reaction("✅")
        await message.add_reaction("❌")
        
        def check(reaction, user):
            return user == ctx.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == message.id
        
        try:
            reaction, user = await bot.wait_for('reaction_add', timeout=30.0, check=check)
            
            if str(reaction.emoji) == "✅":
                await emergency_shutdown(bot, reason)
                await ctx.followup.send("🚨 Emergency shutdown procedures completed!")
                await log_user_activity(str(ctx.author.id), ctx.author.display_name, "emergency_shutdown", reason)
            else:
                await ctx.followup.send("❌ Emergency shutdown cancelled.")
                
        except asyncio.TimeoutError:
            await ctx.followup.send("❌ Emergency shutdown confirmation timed out.")
            
    except Exception as e:
        print(f"❌ Error in emergency command: {e}")
        await ctx.reply("❌ Failed to initiate emergency procedures.")

@bot.command(name="validate", description="Validate data integrity (Admin only)")
@commands.has_permissions(administrator=True)
async def validate_command(ctx):
    """Validate user data integrity."""
    try:
        is_valid = validate_user_data_integrity()
        
        if is_valid:
            await ctx.reply("✅ User data integrity check passed!")
        else:
            await ctx.reply("❌ User data integrity check failed! Check logs for details.")
            
        await log_user_activity(str(ctx.author.id), ctx.author.display_name, "data_validation")
        
    except Exception as e:
        print(f"❌ Error in validate command: {e}")
        await ctx.reply("❌ Failed to validate data integrity.")

# USER INFO COMMANDS
@bot.command(name="userstats", description="Show detailed stats for a specific user")
async def user_stats_command(ctx, user: discord.Member = None):
    """Show detailed statistics for a specific user."""
    target_user = user or ctx.author
    
    try:
        user_embed = await create_detailed_user_stats(bot, str(target_user.id))
        if user_embed:
            await ctx.reply(embed=user_embed)
        else:
            await ctx.reply(f"❌ No statistics found for {target_user.display_name}.")
    except Exception as e:
        print(f"❌ Error in userstats command: {e}")
        await ctx.reply("❌ Failed to retrieve user statistics.")

@bot.command(name="leaderboard", aliases=["lb", "top"], description="Show various leaderboards")
async def leaderboard_command(ctx, board_type: str = "help"):
    """Show different types of leaderboards."""
    if board_type.lower() == "help":
        embed = discord.Embed(
            title="🏆 Available Leaderboards",
            description="Use `!leaderboard <type>` to view specific leaderboards:",
            color=0xf39c12
        )
        embed.add_field(
            name="📊 Available Types",
            value="• `packs` - Top pack openers\n"
                  "• `time` - Most active users\n"
                  "• `efficiency` - Best pack efficiency\n"
                  "• `miss` - Best/worst verifiers\n"
                  "• `farm` - Top farmers",
            inline=False
        )
        await ctx.reply(embed=embed)
        return
    
    # Implementation would depend on creating specific leaderboard functions
    await ctx.reply(f"🚧 Leaderboard type '{board_type}' is coming soon!")

# STATUS COMMANDS
@bot.command(name="status", description="Show bot status and uptime")
async def status_command(ctx):
    """Show bot status information."""
    try:
        # Calculate uptime
        uptime = datetime.datetime.now() - bot.start_time if hasattr(bot, 'start_time') else datetime.timedelta(0)
        
        # Get basic stats
        active_users = await get_active_users(True, False)
        all_users = await get_all_users()
        
        embed = discord.Embed(
            title="🤖 Bot Status",
            color=0x2ecc71
        )
        
        embed.add_field(
            name="⏱️ Uptime",
            value=f"{uptime.days}d {uptime.seconds//3600}h {(uptime.seconds//60)%60}m",
            inline=True
        )
        
        embed.add_field(
            name="👥 Users",
            value=f"Active: {len(active_users)}\nTotal: {len(all_users)}",
            inline=True
        )
        
        embed.add_field(
            name="📊 Last Stats",
            value=last_stats_time.strftime('%H:%M:%S') if last_stats_time else "Never",
            inline=True
        )
        
        embed.add_field(
            name="🔄 Tasks",
            value=f"Stats: {'✅' if stats_sender.is_running() else '❌'}\n"
                  f"Cleanup: {'✅' if user_cleanup.is_running() else '❌'}",
            inline=True
        )
        
        embed.set_footer(text=f"Bot ID: {bot.user.id}")
        await ctx.reply(embed=embed)
        
    except Exception as e:
        print(f"❌ Error in status command: {e}")
        await ctx.reply("❌ Failed to retrieve bot status.")

# ERROR HANDLERS FOR SPECIFIC COMMANDS
@force_stats_command.error
@cleanup_command.error
@backup_command.error
@health_command.error
@emergency_command.error
@validate_command.error
async def admin_command_error(ctx, error):
    """Handle errors for admin commands."""
    if isinstance(error, commands.MissingPermissions):
        await ctx.reply("❌ You need administrator permissions to use this command.")
    else:
        await ctx.reply("❌ An error occurred while executing the admin command.")

# STARTUP INITIALIZATION
async def initialize_bot():
    """Initialize bot data and settings."""
    try:
        # Set start time
        bot.start_time = datetime.datetime.now()
        
        # Create necessary directories
        os.makedirs('data', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        os.makedirs('backups', exist_ok=True)
        
        # Start data backup task if configured
        if hasattr(config, 'enable_auto_backup') and config.enable_auto_backup:
            if not data_backup.is_running():
                data_backup.start()
                print('💾 Data backup task started')
        
        print('✅ Bot initialization completed')
        
    except Exception as e:
        print(f'❌ Error during bot initialization: {e}')

# RUN THE BOT
if __name__ == "__main__":
    try:
        # Initialize bot
        asyncio.run(initialize_bot())
        
        # Run the bot
        bot.run(config.discord_token)
        
    except KeyboardInterrupt:
        print("\n🛑 Bot shutdown requested by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        traceback.print_exc()
    finally:
        print("👋 Bot shutdown complete")