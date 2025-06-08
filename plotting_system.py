import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import io
import discord
import logging
import sqlite3

class PlottingSystem:
    def __init__(self, db_manager):
        self.db = db_manager
        # Import here to avoid circular import
        from heartbeat_analytics import HeartbeatAnalytics
        self.analytics = HeartbeatAnalytics(db_manager)
        self.logger = logging.getLogger(__name__)
        
        # Set up matplotlib style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")

    async def plot_user_timeline(self, discord_id: int, days_back: int = 7) -> discord.File:
        """Create a timeline plot for a specific user"""
        try:
            runs = self.analytics.detect_runs(discord_id, days_back)
            user_data = self.db.get_user(discord_id)
            
            if not runs:
                return await self._create_no_data_plot(f"No data found for user")
            
            # Prepare data
            timestamps = []
            packs_cumulative = []
            instances = []
            ppm_values = []
            
            cumulative_packs = 0
            for run in runs:
                # Create time series for this run
                run_duration = (run.end_time - run.start_time).total_seconds() / 3600  # hours
                time_points = np.linspace(0, run_duration, max(2, int(run_duration * 2)))  # 2 points per hour minimum
                
                for i, time_offset in enumerate(time_points):
                    timestamp = run.start_time + timedelta(hours=time_offset)
                    timestamps.append(timestamp)
                    
                    # Linear interpolation of packs
                    packs_at_time = run.start_packs + (run.total_packs * time_offset / run_duration)
                    packs_cumulative.append(cumulative_packs + packs_at_time)
                    
                    instances.append(run.average_instances)
                    ppm_values.append(run.packs_per_minute)
                
                cumulative_packs += run.total_packs
            
            # Create the plot
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
            
            # Plot 1: Cumulative Packs
            ax1.plot(timestamps, packs_cumulative, 'b-', linewidth=2, marker='o', markersize=3)
            ax1.set_title(f'Cumulative Packs - {user_data.get("display_name", "User")}', fontsize=14, fontweight='bold')
            ax1.set_ylabel('Total Packs')
            ax1.grid(True, alpha=0.3)
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
            
            # Plot 2: Instances Over Time
            ax2.plot(timestamps, instances, 'g-', linewidth=2, marker='s', markersize=3)
            ax2.set_title('Instances Over Time', fontsize=14, fontweight='bold')
            ax2.set_ylabel('Number of Instances')
            ax2.grid(True, alpha=0.3)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
            
            # Plot 3: Packs Per Minute Distribution
            ppm_clean = [p for p in ppm_values if p > 0]
            if ppm_clean:
                ax3.hist(ppm_clean, bins=20, alpha=0.7, color='orange', edgecolor='black')
                ax3.axvline(np.mean(ppm_clean), color='red', linestyle='--', 
                           label=f'Avg: {np.mean(ppm_clean):.2f}')
                ax3.set_title('Packs/Min Distribution', fontsize=14, fontweight='bold')
                ax3.set_xlabel('Packs per Minute')
                ax3.set_ylabel('Frequency')
                ax3.legend()
                ax3.grid(True, alpha=0.3)
            
            # Plot 4: Performance Metrics
            stats = self.analytics.get_user_statistics(discord_id, days_back)
            metrics = ['Efficiency', 'Consistency', 'Avg PPM', 'Avg Instances']
            values = [
                stats.efficiency_score,
                stats.consistency_score,
                stats.average_packs_per_minute * 10,  # Scale for visibility
                stats.average_instances * 10  # Scale for visibility
            ]
            
            bars = ax4.bar(metrics, values, color=['skyblue', 'lightgreen', 'orange', 'pink'])
            ax4.set_title('Performance Metrics', fontsize=14, fontweight='bold')
            ax4.set_ylabel('Score/Value')
            
            # Add value labels on bars
            for bar, value in zip(bars, values):
                height = bar.get_height()
                ax4.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                        f'{value:.1f}', ha='center', va='bottom')
            
            plt.tight_layout()
            
            # Convert to Discord file
            return await self._fig_to_discord_file(fig, f"user_timeline_{discord_id}.png")
            
        except Exception as e:
            self.logger.error(f"Error creating user timeline plot: {e}")
            return await self._create_error_plot("Error creating user timeline")

    async def plot_server_overview(self, days_back: int = 7) -> discord.File:
        """Create a server overview plot"""
        try:
            server_stats = self.analytics.get_server_statistics(days_back)
            timeline_data = server_stats.timeline_data
            
            if not timeline_data['timestamps']:
                return await self._create_no_data_plot("No server data available")
            
            # Convert timestamps to datetime objects
            timestamps = [datetime.fromisoformat(ts) for ts in timeline_data['timestamps']]
            
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
            
            # Plot 1: Active Users Over Time
            ax1.plot(timestamps, timeline_data['active_users'], 'b-', linewidth=2, marker='o', markersize=4)
            ax1.set_title('Active Users Over Time', fontsize=14, fontweight='bold')
            ax1.set_ylabel('Active Users')
            ax1.grid(True, alpha=0.3)
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # Plot 2: Average Instances
            ax2.plot(timestamps, timeline_data['avg_instances'], 'g-', linewidth=2, marker='s', markersize=4)
            ax2.set_title('Average Instances Per User', fontsize=14, fontweight='bold')
            ax2.set_ylabel('Avg Instances')
            ax2.grid(True, alpha=0.3)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            # Plot 3: Top Performers
            if server_stats.top_performers:
                names = [p['name'][:10] for p in server_stats.top_performers]  # Truncate names
                efficiencies = [p['efficiency'] for p in server_stats.top_performers]
                
                bars = ax3.barh(names, efficiencies, color='skyblue')
                ax3.set_title('Top Performers (Efficiency)', fontsize=14, fontweight='bold')
                ax3.set_xlabel('Efficiency Score')
                
                # Add value labels
                for bar, efficiency in zip(bars, efficiencies):
                    width = bar.get_width()
                    ax3.text(width + 0.1, bar.get_y() + bar.get_height()/2,
                            f'{efficiency:.1f}', ha='left', va='center')
            
            # Plot 4: Current Server Stats (Pie Chart)
            current_stats = [
                server_stats.active_users,
                max(0, 20 - server_stats.active_users)  # Assume max 20 for visualization
            ]
            labels = ['Active Users', 'Inactive Slots']
            colors = ['lightgreen', 'lightcoral']
            
            ax4.pie(current_stats, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
            ax4.set_title('Current Server Activity', fontsize=14, fontweight='bold')
            
            # Add text box with key stats
            stats_text = f"""Server Statistics:
Active Users: {server_stats.active_users}
Total Instances: {server_stats.total_instances}
Packs/Hour: {server_stats.total_packs_per_hour:.1f}
Avg Efficiency: {server_stats.average_efficiency:.1f}"""
            
            ax4.text(1.3, 0.5, stats_text, transform=ax4.transAxes, fontsize=10,
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.8))
            
            plt.tight_layout()
            
            return await self._fig_to_discord_file(fig, "server_overview.png")
            
        except Exception as e:
            self.logger.error(f"Error creating server overview plot: {e}")
            return await self._create_error_plot("Error creating server overview")

    async def plot_godpack_analysis(self, days_back: int = 30) -> discord.File:
        """Create god pack analysis plots"""
        try:
            # Get god pack data from database
            with self.db.lock:
                conn = sqlite3.connect(self.db.db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                since_date = datetime.now() - timedelta(days=days_back)
                
                cursor.execute('''
                    SELECT g.*, gs.probability_alive
                    FROM godpacks g
                    LEFT JOIN gp_statistics gs ON g.id = gs.gp_id
                    WHERE g.timestamp >= ?
                    ORDER BY g.timestamp ASC
                ''', (since_date,))
                
                godpacks = cursor.fetchall()
                conn.close()
            
            if not godpacks:
                return await self._create_no_data_plot("No god pack data available")
            
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
            
            # Prepare data
            timestamps = [datetime.fromisoformat(gp['timestamp']) for gp in godpacks]
            states = [gp['state'] for gp in godpacks]
            probabilities = [gp['probability_alive'] or 50 for gp in godpacks]
            
            # Plot 1: God Packs Over Time
            ax1.hist([t.date() for t in timestamps], bins=days_back//3, alpha=0.7, color='purple')
            ax1.set_title('God Packs Found Over Time', fontsize=14, fontweight='bold')
            ax1.set_xlabel('Date')
            ax1.set_ylabel('Number of God Packs')
            ax1.grid(True, alpha=0.3)
            
            # Plot 2: State Distribution
            state_counts = {}
            for state in states:
                state_counts[state] = state_counts.get(state, 0) + 1
            
            if state_counts:
                ax2.pie(state_counts.values(), labels=state_counts.keys(), autopct='%1.1f%%', startangle=90)
                ax2.set_title('God Pack State Distribution', fontsize=14, fontweight='bold')
            
            # Plot 3: Probability Distribution
            ax3.hist(probabilities, bins=20, alpha=0.7, color='orange', edgecolor='black')
            ax3.set_title('Probability Distribution', fontsize=14, fontweight='bold')
            ax3.set_xlabel('Probability (%)')
            ax3.set_ylabel('Frequency')
            ax3.grid(True, alpha=0.3)
            
            # Plot 4: Success Rate Trend
            daily_data = {}
            for i, gp in enumerate(godpacks):
                date = datetime.fromisoformat(gp['timestamp']).date()
                if date not in daily_data:
                    daily_data[date] = {'total': 0, 'alive': 0}
                daily_data[date]['total'] += 1
                if gp['state'] in ['ALIVE', 'TESTING']:
                    daily_data[date]['alive'] += 1
            
            dates = sorted(daily_data.keys())
            success_rates = [daily_data[date]['alive'] / daily_data[date]['total'] * 100 
                           for date in dates if daily_data[date]['total'] > 0]
            
            if dates and success_rates:
                ax4.plot(dates, success_rates, 'g-', linewidth=2, marker='o')
                ax4.set_title('Daily Success Rate', fontsize=14, fontweight='bold')
                ax4.set_xlabel('Date')
                ax4.set_ylabel('Success Rate (%)')
                ax4.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            return await self._fig_to_discord_file(fig, "godpack_analysis.png")
            
        except Exception as e:
            self.logger.error(f"Error creating god pack analysis: {e}")
            return await self._create_error_plot("Error creating god pack analysis")

    async def plot_probability_trends(self, gp_id: int) -> discord.File:
        """Generate probability trend plot for a god pack"""
        try:
            # Get test results for this god pack
            test_results = self.db.get_test_results(gp_id)
            
            if not test_results:
                return await self._create_no_data_plot(f"No test data for GP {gp_id}")
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
            
            # Calculate probability over time
            timestamps = []
            probabilities = []
            
            # Import here to avoid circular import
            from probability_calculator import ProbabilityCalculator
            prob_calc = ProbabilityCalculator(self.db)
            
            # Simulate probability changes over time
            for i in range(len(test_results) + 1):
                if i == 0:
                    # Initial probability
                    timestamps.append(test_results[0].timestamp if test_results else datetime.now())
                    probabilities.append(100.0)
                else:
                    # Recalculate after each test
                    test_subset = test_results[:i]
                    # This would need actual implementation of incremental probability calculation
                    prob = 100.0 - (i * 20)  # Simplified for demo
                    timestamps.append(test_results[i-1].timestamp)
                    probabilities.append(max(0, prob))
            
            # Plot 1: Probability Over Time
            ax1.plot(timestamps, probabilities, 'r-', linewidth=3, marker='o', markersize=6)
            ax1.set_title(f'Probability Trend - GP {gp_id}', fontsize=14, fontweight='bold')
            ax1.set_ylabel('Probability Alive (%)')
            ax1.grid(True, alpha=0.3)
            ax1.set_ylim(0, 105)
            
            # Plot 2: Test Timeline
            test_types = []
            test_times = []
            
            for test in test_results:
                test_types.append(test.test_type.value)
                test_times.append(test.timestamp)
            
            # Create scatter plot for tests
            colors = ['red' if t == 'MISS' else 'orange' for t in test_types]
            ax2.scatter(test_times, range(len(test_times)), c=colors, s=100, alpha=0.7)
            
            for i, (time, test_type) in enumerate(zip(test_times, test_types)):
                ax2.annotate(test_type, (time, i), xytext=(5, 0), textcoords='offset points')
            
            ax2.set_title('Test Timeline', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Time')
            ax2.set_ylabel('Test Number')
            ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            return await self._fig_to_discord_file(fig, f"probability_trends_{gp_id}.png")
            
        except Exception as e:
            self.logger.error(f"Error creating probability trends plot: {e}")
            return await self._create_error_plot("Error creating probability trends")

    async def _fig_to_discord_file(self, fig, filename: str) -> discord.File:
        """Convert matplotlib figure to Discord file"""
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        buf.seek(0)
        plt.close(fig)  # Free memory
        
        return discord.File(buf, filename=filename)

    async def _create_no_data_plot(self, message: str) -> discord.File:
        """Create a simple plot indicating no data available"""
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, message, ha='center', va='center', fontsize=16,
                bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgray", alpha=0.8))
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        
        return await self._fig_to_discord_file(fig, "no_data.png")

    async def _create_error_plot(self, message: str) -> discord.File:
        """Create a simple plot indicating an error occurred"""
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"❌ {message}", ha='center', va='center', fontsize=16, color='red',
                bbox=dict(boxstyle="round,pad=0.5", facecolor="mistyrose", alpha=0.8))
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        
        return await self._fig_to_discord_file(fig, "error.png")