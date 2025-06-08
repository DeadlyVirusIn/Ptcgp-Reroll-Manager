import discord
from discord.ext import commands
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List
import logging

try:
    import config
    COMMAND_COOLDOWN = getattr(config, 'COMMAND_COOLDOWN_SECONDS', 2)
except ImportError:
    COMMAND_COOLDOWN = 2

class EnhancedBotCommands(commands.Cog):
    def __init__(self, bot, db_manager):
        self.bot = bot
        self.db = db_manager
        self.logger = logging.getLogger(__name__)
        
        # FIXED: Use lazy loading to avoid circular imports
        self._probability_calc = None
        self._analytics = None
        self._plotting = None
        self._expiration_manager = None
        self._sheets_integration = None

    @property
    def probability_calc(self):
        """Lazy load ProbabilityCalculator to avoid circular imports"""
        if self._probability_calc is None:
            try:
                from probability_calculator import ProbabilityCalculator
                self._probability_calc = ProbabilityCalculator(self.db)
            except ImportError:
                self.logger.warning("ProbabilityCalculator not available")
                return None
        return self._probability_calc

    @property
    def analytics(self):
        """Lazy load HeartbeatAnalytics to avoid circular imports"""
        if self._analytics is None:
            try:
                from heartbeat_analytics import HeartbeatAnalytics
                self._analytics = HeartbeatAnalytics(self.db)
            except ImportError:
                self.logger.warning("HeartbeatAnalytics not available")
                return None
        return self._analytics

    @property
    def plotting(self):
        """Lazy load PlottingSystem to avoid circular imports"""
        if self._plotting is None:
            try:
                from plotting_system import PlottingSystem
                self._plotting = PlottingSystem(self.db)
            except ImportError:
                self.logger.warning("PlottingSystem not available")
                return None
        return self._plotting

    @property
    def expiration_manager(self):
        """Lazy load ExpirationManager to avoid circular imports"""
        if self._expiration_manager is None:
            try:
                from expiration_manager import ExpirationManager
                self._expiration_manager = ExpirationManager(self.db, self.bot)
            except ImportError:
                self.logger.warning("ExpirationManager not available")
                return None
        return self._expiration_manager

    @property
    def sheets_integration(self):
        """Lazy load GoogleSheetsIntegration to avoid circular imports"""
        if self._sheets_integration is None:
            try:
                from google_sheets_integration import GoogleSheetsIntegration
                self._sheets_integration = GoogleSheetsIntegration(self.db)
            except ImportError:
                self.logger.warning("GoogleSheetsIntegration not available")
                return None
        return self._sheets_integration

    # Advanced Probability Commands
    @commands.slash_command(name="probability", description="Calculate probability for a god pack")
    async def probability(self, ctx, gp_id: int):
        """Calculate and display the probability that a god pack is alive"""
        try:
            await ctx.defer()
            
            if not self.probability_calc:
                await ctx.followup.send("❌ Probability calculator not available.")
                return
            
            # Import required enums locally to avoid circular imports
            try:
                from database_manager import GPState, TestType
            except ImportError:
                await ctx.followup.send("❌ Database manager not available.")
                return
            
            result = self.probability_calc.calculate_godpack_probability(gp_id, force_recalculate=True)
            summary = self.probability_calc.get_probability_summary(gp_id)
            
            embed = discord.Embed(
                title=f"🎯 Probability Analysis - {summary['godpack'].name}",
                color=self._get_probability_color(result.probability_alive),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="📊 Probability Alive",
                value=f"**{result.probability_alive:.1f}%**",
                inline=True
            )
            
            embed.add_field(
                name="🎯 Confidence Level",
                value=f"**{result.confidence_level:.1f}%**",
                inline=True
            )
            
            embed.add_field(
                name="🧪 Total Tests",
                value=f"**{result.total_tests}**",
                inline=True
            )
            
            embed.add_field(
                name="❌ Miss Tests",
                value=f"{result.miss_tests}",
                inline=True
            )
            
            embed.add_field(
                name="👻 No-Show Tests",
                value=f"{result.noshow_tests}",
                inline=True
            )
            
            embed.add_field(
                name="💡 Recommendation",
                value=summary['recommendation'],
                inline=False
            )
            
            # Add member breakdown if available
            if summary['member_details']:
                member_text = []
                for member_id, details in summary['member_details'].items():
                    member_text.append(f"**{details['name']}**: {details['probability']:.1f}% ({len(details['tests'])} tests)")
                
                if member_text:
                    embed.add_field(
                        name="👥 Member Breakdown",
                        value="\n".join(member_text[:5]),  # Limit to 5 members
                        inline=False
                    )
            
            embed.set_footer(text=f"GP ID: {gp_id} | Last calculated: {result.last_calculated.strftime('%H:%M:%S')}")
            
            await ctx.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in probability command: {e}")
            await ctx.followup.send("❌ Error calculating probability. Please check the GP ID.")

    @commands.slash_command(name="advancedmiss", description="Add a miss test with advanced tracking")
    async def advanced_miss(self, ctx, gp_id: int):
        """Add a miss test and calculate updated probability"""
        try:
            await ctx.defer()
            
            if not self.probability_calc:
                await ctx.followup.send("❌ Probability calculator not available.")
                return
            
            # Import TestType locally
            try:
                from database_manager import TestType
            except ImportError:
                await ctx.followup.send("❌ Database manager not available.")
                return
            
            result = self.probability_calc.add_test_and_calculate(
                ctx.author.id, gp_id, TestType.MISS
            )
            
            embed = discord.Embed(
                title="❌ Miss Test Added",
                description=f"Updated probability: **{result.probability_alive:.1f}%**",
                color=discord.Color.red(),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="Impact",
                value=f"Total tests: {result.total_tests}\nConfidence: {result.confidence_level:.1f}%",
                inline=False
            )
            
            embed.set_footer(text=f"GP ID: {gp_id}")
            
            await ctx.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in advanced miss command: {e}")
            await ctx.followup.send("❌ Error adding miss test.")

    @commands.slash_command(name="advancednoshow", description="Add a no-show test with slot/friend data")
    async def advanced_noshow(self, ctx, gp_id: int, open_slots: int, number_friends: int):
        """Add a no-show test with detailed probability calculation"""
        try:
            await ctx.defer()
            
            if not self.probability_calc:
                await ctx.followup.send("❌ Probability calculator not available.")
                return
            
            # Import TestType locally
            try:
                from database_manager import TestType
            except ImportError:
                await ctx.followup.send("❌ Database manager not available.")
                return
            
            # Calculate no-show probability first
            noshow_prob = self.probability_calc.calculate_noshow_probability(open_slots, number_friends)
            
            result = self.probability_calc.add_test_and_calculate(
                ctx.author.id, gp_id, TestType.NOSHOW, open_slots, number_friends
            )
            
            embed = discord.Embed(
                title="👻 No-Show Test Added",
                description=f"Updated probability: **{result.probability_alive:.1f}%**",
                color=discord.Color.orange(),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="Test Details",
                value=f"Open slots: {open_slots}\nFriends: {number_friends}\nNo-show impact: {noshow_prob:.1f}%",
                inline=True
            )
            
            embed.add_field(
                name="Overall Impact",
                value=f"Total tests: {result.total_tests}\nConfidence: {result.confidence_level:.1f}%",
                inline=True
            )
            
            embed.set_footer(text=f"GP ID: {gp_id}")
            
            await ctx.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in advanced no-show command: {e}")
            await ctx.followup.send("❌ Error adding no-show test.")

    # Advanced Analytics Commands
    @commands.slash_command(name="userstats", description="Get detailed user statistics")
    async def user_stats(self, ctx, user: Optional[discord.Member] = None, days: int = 7):
        """Get detailed statistics for a user"""
        try:
            await ctx.defer()
            
            if not self.analytics:
                await ctx.followup.send("❌ Analytics not available.")
                return
            
            target_user = user or ctx.author
            stats = self.analytics.get_user_statistics(target_user.id, days)
            
            embed = discord.Embed(
                title=f"📊 User Statistics - {stats.display_name}",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="🏃 Activity",
                value=f"Status: **{stats.status.title()}**\nTotal runs: **{stats.total_runs}**\nRuntime: **{stats.total_runtime_hours:.1f}h**",
                inline=True
            )
            
            embed.add_field(
                name="📦 Performance",
                value=f"Total packs: **{stats.total_packs}**\nAvg PPM: **{stats.average_packs_per_minute:.2f}**\nEfficiency: **{stats.efficiency_score:.1f}**",
                inline=True
            )
            
            embed.add_field(
                name="🎯 Quality",
                value=f"Avg instances: **{stats.average_instances:.1f}**\nPeak instances: **{stats.peak_instances}**\nConsistency: **{stats.consistency_score:.1f}%**",
                inline=True
            )
            
            # Add performance trend
            if stats.total_runs > 1:
                embed.add_field(
                    name="📈 Recent Activity",
                    value=f"Last active: {stats.last_active.strftime('%m/%d %H:%M')}",
                    inline=False
                )
            
            embed.set_footer(text=f"Data from last {days} days")
            
            await ctx.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in user stats command: {e}")
            await ctx.followup.send("❌ Error retrieving user statistics.")

    @commands.slash_command(name="serverstats", description="Get server-wide statistics")
    async def server_stats(self, ctx, days: int = 7):
        """Get comprehensive server statistics"""
        try:
            await ctx.defer()
            
            if not self.analytics:
                await ctx.followup.send("❌ Analytics not available.")
                return
            
            server_stats = self.analytics.get_server_statistics(days)
            
            embed = discord.Embed(
                title="🌐 Server Statistics",
                color=discord.Color.green(),
                timestamp=datetime.now()
            )
            
            embed.add_field(
                name="👥 Current Activity",
                value=f"Active users: **{server_stats.active_users}**\nTotal instances: **{server_stats.total_instances}**",
                inline=True
            )
            
            embed.add_field(
                name="📦 Performance",
                value=f"Packs/hour: **{server_stats.total_packs_per_hour:.1f}**\nAvg efficiency: **{server_stats.average_efficiency:.1f}**",
                inline=True
            )
            
            embed.add_field(
                name="🏆 Top Performers",
                value="\n".join([f"**{p['name']}**: {p['efficiency']:.1f}" for p in server_stats.top_performers[:3]]),
                inline=False
            )
            
            embed.set_footer(text=f"Data from last {days} days")
            
            await ctx.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in server stats command: {e}")
            await ctx.followup.send("❌ Error retrieving server statistics.")

    @commands.slash_command(name="leaderboard", description="Show performance leaderboard")
    async def leaderboard(self, ctx, metric: str = "efficiency", days: int = 7):
        """Show leaderboard for specified metric"""
        try:
            await ctx.defer()
            
            if not self.analytics:
                await ctx.followup.send("❌ Analytics not available.")
                return
            
            valid_metrics = ["efficiency", "total_packs", "runtime", "consistency"]
            if metric not in valid_metrics:
                await ctx.followup.send(f"❌ Invalid metric. Use: {', '.join(valid_metrics)}")
                return
            
            leaderboard = self.analytics.generate_leaderboard(metric, days, limit=10)
            
            if not leaderboard:
                await ctx.followup.send("📊 No data available for leaderboard.")
                return
            
            embed = discord.Embed(
                title=f"🏆 Leaderboard - {metric.title()}",
                color=discord.Color.gold(),
                timestamp=datetime.now()
            )
            
            leaderboard_text = []
            for entry in leaderboard:
                rank_emoji = ["🥇", "🥈", "🥉"][entry['rank']-1] if entry['rank'] <= 3 else f"{entry['rank']}."
                leaderboard_text.append(f"{rank_emoji} **{entry['name']}**: {entry['value']:.1f}")
            
            embed.description = "\n".join(leaderboard_text)
            embed.set_footer(text=f"Data from last {days} days")
            
            await ctx.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in leaderboard command: {e}")
            await ctx.followup.send("❌ Error generating leaderboard.")

    # Advanced Plotting Commands
    @commands.slash_command(name="plot_user", description="Generate user performance plots")
    async def plot_user(self, ctx, user: Optional[discord.Member] = None, days: int = 7):
        """Generate detailed plots for a user"""
        try:
            await ctx.defer()
            
            if not self.plotting:
                await ctx.followup.send("❌ Plotting system not available.")
                return
            
            target_user = user or ctx.author
            plot_file = await self.plotting.plot_user_timeline(target_user.id, days)
            
            embed = discord.Embed(
                title=f"📈 Performance Analysis - {target_user.display_name}",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            embed.set_image(url="attachment://user_timeline.png")
            embed.set_footer(text=f"Data from last {days} days")
            
            await ctx.followup.send(embed=embed, file=plot_file)
            
        except Exception as e:
            self.logger.error(f"Error in plot user command: {e}")
            await ctx.followup.send("❌ Error generating user plot.")

    @commands.slash_command(name="plot_server", description="Generate server overview plots")
    async def plot_server(self, ctx, days: int = 7):
        """Generate server overview plots"""
        try:
            await ctx.defer()
            
            if not self.plotting:
                await ctx.followup.send("❌ Plotting system not available.")
                return
            
            plot_file = await self.plotting.plot_server_overview(days)
            
            embed = discord.Embed(
                title="🌐 Server Overview",
                color=discord.Color.green(),
                timestamp=datetime.now()
            )
            
            embed.set_image(url="attachment://server_overview.png")
            embed.set_footer(text=f"Data from last {days} days")
            
            await ctx.followup.send(embed=embed, file=plot_file)
            
        except Exception as e:
            self.logger.error(f"Error in plot server command: {e}")
            await ctx.followup.send("❌ Error generating server plot.")

    @commands.slash_command(name="plot_godpacks", description="Generate god pack analysis plots")
    async def plot_godpacks(self, ctx, days: int = 30):
        """Generate god pack analysis plots"""
        try:
            await ctx.defer()
            
            if not self.plotting:
                await ctx.followup.send("❌ Plotting system not available.")
                return
            
            plot_file = await self.plotting.plot_godpack_analysis(days)
            
            embed = discord.Embed(
                title="🎁 God Pack Analysis",
                color=discord.Color.purple(),
                timestamp=datetime.now()
            )
            
            embed.set_image(url="attachment://godpack_analysis.png")
            embed.set_footer(text=f"Data from last {days} days")
            
            await ctx.followup.send(embed=embed, file=plot_file)
            
        except Exception as e:
            self.logger.error(f"Error in plot god packs command: {e}")
            await ctx.followup.send("❌ Error generating god pack analysis.")

    @commands.slash_command(name="plot_probability", description="Show probability trends for a god pack")
    async def plot_probability(self, ctx, gp_id: int):
        """Generate probability trend plot for a god pack"""
        try:
            await ctx.defer()
            
            if not self.plotting:
                await ctx.followup.send("❌ Plotting system not available.")
                return
            
            plot_file = await self.plotting.plot_probability_trends(gp_id)
            
            embed = discord.Embed(
                title=f"📊 Probability Trends - GP {gp_id}",
                color=discord.Color.orange(),
                timestamp=datetime.now()
            )
            
            embed.set_image(url="attachment://probability_trends.png")
            
            await ctx.followup.send(embed=embed, file=plot_file)
            
        except Exception as e:
            self.logger.error(f"Error in plot probability command: {e}")
            await ctx.followup.send("❌ Error generating probability plot.")

    # Expiration Management Commands
    @commands.slash_command(name="expiring", description="Show god packs expiring soon")
    async def expiring(self, ctx, days: int = 3):
        """Show god packs expiring in the next few days"""
        try:
            await ctx.defer()
            
            if not self.expiration_manager:
                await ctx.followup.send("❌ Expiration manager not available.")
                return
            
            summary = await self.expiration_manager.get_expiration_summary(days)
            
            if summary['total_expiring'] == 0:
                await ctx.followup.send(f"✅ No god packs expiring in the next {days} days.")
                return
            
            embed = discord.Embed(
                title=f"⏰ Expiring God Packs ({days} days)",
                color=discord.Color.orange(),
                timestamp=datetime.now()
            )
            
            # Summary by state
            if summary['by_state']:
                state_text = []
                for state, count in summary['by_state'].items():
                    state_text.append(f"**{state}**: {count}")
                
                embed.add_field(
                    name="📊 By State",
                    value="\n".join(state_text),
                    inline=True
                )
            
            # Detailed list
            if summary['detailed_list']:
                detail_text = []
                for gp in summary['detailed_list'][:10]:  # Limit to 10
                    hours = gp['hours_remaining']
                    detail_text.append(f"**{gp['name']}** ({gp['state']}) - {hours:.1f}h")
                
                embed.add_field(
                    name="🎁 Expiring Soon",
                    value="\n".join(detail_text),
                    inline=False
                )
            
            embed.set_footer(text=f"Total: {summary['total_expiring']} god packs")
            
            await ctx.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in expiring command: {e}")
            await ctx.followup.send("❌ Error retrieving expiration data.")

    @commands.slash_command(name="extend_expiration", description="Extend expiration time for a god pack")
    @commands.has_permissions(manage_messages=True)
    async def extend_expiration(self, ctx, gp_id: int, hours: int):
        """Manually extend the expiration time of a god pack"""
        try:
            await ctx.defer()
            
            if not self.expiration_manager:
                await ctx.followup.send("❌ Expiration manager not available.")
                return
            
            if hours <= 0 or hours > 168:  # Max 1 week
                await ctx.followup.send("❌ Hours must be between 1 and 168 (1 week).")
                return
            
            success = await self.expiration_manager.extend_expiration(gp_id, hours)
            
            if success:
                embed = discord.Embed(
                    title="⏰ Expiration Extended",
                    description=f"God pack {gp_id} expiration extended by {hours} hours.",
                    color=discord.Color.green(),
                    timestamp=datetime.now()
                )
                await ctx.followup.send(embed=embed)
            else:
                await ctx.followup.send("❌ Failed to extend expiration. Check GP ID.")
                
        except Exception as e:
            self.logger.error(f"Error in extend expiration command: {e}")
            await ctx.followup.send("❌ Error extending expiration.")

    @commands.slash_command(name="force_expire", description="Manually expire a god pack")
    @commands.has_permissions(manage_messages=True)
    async def force_expire(self, ctx, gp_id: int, reason: str = "Manual expiration"):
        """Manually expire a god pack immediately"""
        try:
            await ctx.defer()
            
            if not self.expiration_manager:
                await ctx.followup.send("❌ Expiration manager not available.")
                return
            
            success = await self.expiration_manager.force_expire_godpack(gp_id, reason)
            
            if success:
                embed = discord.Embed(
                    title="🔧 God Pack Manually Expired",
                    description=f"God pack {gp_id} has been manually expired.",
                    color=discord.Color.red(),
                    timestamp=datetime.now()
                )
                embed.add_field(name="Reason", value=reason, inline=False)
                await ctx.followup.send(embed=embed)
            else:
                await ctx.followup.send("❌ Failed to expire god pack. Check GP ID.")
                
        except Exception as e:
            self.logger.error(f"Error in force expire command: {e}")
            await ctx.followup.send("❌ Error expiring god pack.")

    # Google Sheets Integration Commands
    @commands.slash_command(name="setup_sheets", description="Set up Google Sheets integration")
    @commands.has_permissions(administrator=True)
    async def setup_sheets(self, ctx, spreadsheet_name: str):
        """Set up Google Sheets integration for this server"""
        try:
            await ctx.defer()
            
            if not self.sheets_integration:
                await ctx.followup.send("❌ Google Sheets integration not available.")
                return
            
            success = await self.sheets_integration.setup_guild_spreadsheet(ctx.guild.id, spreadsheet_name)
            
            if success:
                url = await self.sheets_integration.get_spreadsheet_url(ctx.guild.id)
                embed = discord.Embed(
                    title="📊 Google Sheets Integration Setup",
                    description="Successfully set up Google Sheets integration!",
                    color=discord.Color.green(),
                    timestamp=datetime.now()
                )
                if url:
                    embed.add_field(name="Spreadsheet URL", value=url, inline=False)
                await ctx.followup.send(embed=embed)
            else:
                await ctx.followup.send("❌ Failed to set up Google Sheets integration. Check credentials.")
                
        except Exception as e:
            self.logger.error(f"Error in setup sheets command: {e}")
            await ctx.followup.send("❌ Error setting up Google Sheets integration.")

    @commands.slash_command(name="sync_sheets", description="Sync data to Google Sheets")
    @commands.has_permissions(manage_messages=True)
    async def sync_sheets(self, ctx):
        """Manually sync all data to Google Sheets"""
        try:
            await ctx.defer()
            
            if not self.sheets_integration:
                await ctx.followup.send("❌ Google Sheets integration not available.")
                return
            
            results = await self.sheets_integration.full_sync(ctx.guild.id)
            
            embed = discord.Embed(
                title="📊 Google Sheets Sync",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            sync_results = []
            for data_type, success in results.items():
                status = "✅" if success else "❌"
                sync_results.append(f"{status} {data_type.title()}")
            
            embed.add_field(
                name="Sync Results",
                value="\n".join(sync_results),
                inline=False
            )
            
            success_count = sum(results.values())
            embed.set_footer(text=f"Successfully synced {success_count}/{len(results)} data types")
            
            await ctx.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in sync sheets command: {e}")
            await ctx.followup.send("❌ Error syncing to Google Sheets.")

    @commands.slash_command(name="anomalies", description="Detect performance anomalies")
    async def anomalies(self, ctx, user: Optional[discord.Member] = None, days: int = 7):
        """Detect anomalies in user performance"""
        try:
            await ctx.defer()
            
            if not self.analytics:
                await ctx.followup.send("❌ Analytics not available.")
                return
            
            target_user = user or ctx.author
            anomalies = self.analytics.detect_anomalies(target_user.id, days)
            
            if not anomalies:
                await ctx.followup.send(f"✅ No anomalies detected for {target_user.display_name}.")
                return
            
            embed = discord.Embed(
                title=f"🔍 Performance Anomalies - {target_user.display_name}",
                color=discord.Color.orange(),
                timestamp=datetime.now()
            )
            
            anomaly_text = []
            for anomaly in anomalies[:5]:  # Limit to 5 most recent
                timestamp = anomaly['timestamp'].strftime('%m/%d %H:%M')
                anomaly_text.append(f"**{timestamp}**: {anomaly['description']}")
            
            embed.description = "\n".join(anomaly_text)
            embed.set_footer(text=f"Found {len(anomalies)} anomalies in last {days} days")
            
            await ctx.followup.send(embed=embed)
            
        except Exception as e:
            self.logger.error(f"Error in anomalies command: {e}")
            await ctx.followup.send("❌ Error detecting anomalies.")

    # Utility Methods
    def _get_probability_color(self, probability: float) -> discord.Color:
        """Get color based on probability value"""
        if probability >= 80:
            return discord.Color.green()
        elif probability >= 50:
            return discord.Color.orange()
        elif probability >= 20:
            return discord.Color.red()
        else:
            return discord.Color.dark_red()

def setup(bot):
    """Setup function for loading this cog"""
    bot.add_cog(EnhancedBotCommands(bot))