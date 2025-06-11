import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
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
import gc
import asyncio
import threading
from functools import wraps
import weakref

# Import configuration
try:
    import config
    MAX_CONCURRENT_PLOTS = getattr(config, 'MAX_CONCURRENT_PLOTS', 3)
    PLOT_CACHE_DURATION = getattr(config, 'PLOT_CACHE_DURATION_MINUTES', 10)
    PLOT_DPI = getattr(config, 'PLOT_DPI', 150)
    PLOT_STYLE = getattr(config, 'PLOT_STYLE', 'seaborn-v0_8')
    PLOT_COLOR_SCHEME = getattr(config, 'PLOT_COLOR_SCHEME', 'husl')
    PLOT_FIGURE_SIZE = getattr(config, 'PLOT_FIGURE_SIZE', (15, 10))
except ImportError:
    MAX_CONCURRENT_PLOTS = 3
    PLOT_CACHE_DURATION = 10
    PLOT_DPI = 150
    PLOT_STYLE = 'seaborn-v0_8'
    PLOT_COLOR_SCHEME = 'husl'
    PLOT_FIGURE_SIZE = (15, 10)

class PlotCache:
    """Cache for generated plots to avoid regeneration"""
    
    def __init__(self, duration_minutes: int = 10):
        self._cache = {}
        self._duration = timedelta(minutes=duration_minutes)
        self._lock = threading.Lock()
    
    def get(self, key: str) -> Optional[bytes]:
        """Get cached plot data"""
        with self._lock:
            if key in self._cache:
                data, timestamp = self._cache[key]
                if datetime.now() - timestamp < self._duration:
                    return data
                else:
                    del self._cache[key]
            return None
    
    def set(self, key: str, data: bytes):
        """Cache plot data"""
        with self._lock:
            self._cache[key] = (data, datetime.now())
    
    def clear_expired(self):
        """Clear expired cache entries"""
        with self._lock:
            now = datetime.now()
            expired_keys = [
                key for key, (_, timestamp) in self._cache.items()
                if now - timestamp >= self._duration
            ]
            for key in expired_keys:
                del self._cache[key]

def memory_safe_plot(func):
    """Decorator to ensure memory safety for plotting functions"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            # Close any existing figures
            plt.close('all')
            
            # Run the plotting function
            result = await func(*args, **kwargs)
            
            # Force garbage collection
            gc.collect()
            
            return result
            
        except Exception as e:
            # Clean up on error
            plt.close('all')
            gc.collect()
            raise e
    
    return wrapper

class PlottingSystem:
    def __init__(self, db_manager):
        self.db = db_manager
        self.logger = logging.getLogger(__name__)
        
        # Import analytics lazily to avoid circular imports
        self._analytics = None
        
        # Plot cache
        self._cache = PlotCache(PLOT_CACHE_DURATION)
        
        # Semaphore for concurrent plot limiting
        self._plot_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PLOTS)
        
        # Set up matplotlib style
        try:
            plt.style.use(PLOT_STYLE)
        except:
            self.logger.warning(f"Plot style '{PLOT_STYLE}' not available, using default")
        
        sns.set_palette(PLOT_COLOR_SCHEME)
        
        # Configure matplotlib for memory efficiency
        matplotlib.rcParams['figure.max_open_warning'] = 5
        matplotlib.rcParams['agg.path.chunksize'] = 10000
    
    @property
    def analytics(self):
        """Lazy load HeartbeatAnalytics to avoid circular imports"""
        if self._analytics is None:
            try:
                from heartbeat_analytics import HeartbeatAnalytics
                self._analytics = HeartbeatAnalytics(self.db)
            except ImportError:
                self.logger.error("HeartbeatAnalytics not available")
                raise
        return self._analytics
    
    async def _fig_to_discord_file(self, fig, filename: str) -> discord.File:
        """Convert matplotlib figure to Discord file with memory management"""
        buf = None
        try:
            # Create buffer
            buf = io.BytesIO()
            
            # Save figure with optimized settings
            fig.savefig(
                buf, 
                format='png', 
                dpi=PLOT_DPI, 
                bbox_inches='tight',
                facecolor='white', 
                edgecolor='none',
                optimize=True,  # Optimize PNG
                metadata={'Software': 'PTCGP Bot'}
            )
            
            # Reset buffer position
            buf.seek(0)
            
            # Create Discord file
            discord_file = discord.File(buf, filename=filename)
            
            return discord_file
            
        finally:
            # Clean up
            plt.close(fig)
            plt.clf()
            plt.cla()
            
            # Force garbage collection
            gc.collect()

    @memory_safe_plot
    async def plot_user_timeline(self, discord_id: int, days_back: int = 7) -> discord.File:
        """Create a timeline plot for a specific user"""
        async with self._plot_semaphore:
            # Check cache
            cache_key = f"user_timeline_{discord_id}_{days_back}"
            cached_data = self._cache.get(cache_key)
            if cached_data:
                return discord.File(io.BytesIO(cached_data), filename=f"user_timeline_{discord_id}.png")
            
            try:
                # Get user data - simplified version for basic functionality
                user_data = self.db.get_user(discord_id)
                if not user_data:
                    return await self._create_no_data_plot(f"No data found for user")
                
                # Create simple figure
                fig, ax = plt.subplots(figsize=(12, 8))
                
                # Simple user info display
                ax.text(0.5, 0.5, 
                       f"User Timeline for {user_data.get('display_name', 'Unknown User')}\n"
                       f"Total Packs: {user_data.get('total_packs', 0)}\n"
                       f"Status: {user_data.get('status', 'Unknown')}",
                       ha='center', va='center', fontsize=14,
                       bbox=dict(boxstyle="round,pad=0.5", facecolor="lightblue", alpha=0.8))
                
                ax.set_xlim(0, 1)
                ax.set_ylim(0, 1)
                ax.set_title(f'User Timeline - {user_data.get("display_name", "User")}', fontsize=16, fontweight='bold')
                ax.axis('off')
                
                # Convert to Discord file
                file = await self._fig_to_discord_file(fig, f"user_timeline_{discord_id}.png")
                
                # Cache the result
                buf = io.BytesIO()
                fig.savefig(buf, format='png', dpi=PLOT_DPI, bbox_inches='tight')
                self._cache.set(cache_key, buf.getvalue())
                
                return file
                
            except Exception as e:
                self.logger.error(f"Error creating user timeline plot: {e}")
                return await self._create_error_plot("Error creating user timeline")
    
    @memory_safe_plot
    async def plot_server_overview(self, days_back: int = 7) -> discord.File:
        """Create a server overview plot"""
        async with self._plot_semaphore:
            # Check cache
            cache_key = f"server_overview_{days_back}"
            cached_data = self._cache.get(cache_key)
            if cached_data:
                return discord.File(io.BytesIO(cached_data), filename="server_overview.png")
            
            try:
                # Get basic server stats
                users = self.db.get_all_users()
                active_users = self.db.get_active_users(60)  # Last hour
                
                # Create figure
                fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=PLOT_FIGURE_SIZE)
                
                # Plot 1: User Status Distribution
                status_counts = {}
                for user in users:
                    status = user.get('status', 'unknown')
                    status_counts[status] = status_counts.get(status, 0) + 1
                
                if status_counts:
                    ax1.pie(status_counts.values(), labels=status_counts.keys(), autopct='%1.1f%%')
                    ax1.set_title('User Status Distribution')
                
                # Plot 2: Active vs Inactive
                active_count = len(active_users)
                inactive_count = len(users) - active_count
                
                ax2.bar(['Active', 'Inactive'], [active_count, inactive_count], 
                       color=['green', 'red'], alpha=0.7)
                ax2.set_title('Active vs Inactive Users')
                ax2.set_ylabel('Number of Users')
                
                # Plot 3: Total Packs Distribution
                pack_counts = [user.get('total_packs', 0) for user in users if user.get('total_packs', 0) > 0]
                if pack_counts:
                    ax3.hist(pack_counts, bins=20, alpha=0.7, color='blue')
                    ax3.set_title('Total Packs Distribution')
                    ax3.set_xlabel('Total Packs')
                    ax3.set_ylabel('Number of Users')
                
                # Plot 4: Server Summary
                ax4.text(0.5, 0.7, f"Total Users: {len(users)}", ha='center', fontsize=12)
                ax4.text(0.5, 0.5, f"Active Users: {active_count}", ha='center', fontsize=12)
                ax4.text(0.5, 0.3, f"Total Packs: {sum(pack_counts)}", ha='center', fontsize=12)
                ax4.set_xlim(0, 1)
                ax4.set_ylim(0, 1)
                ax4.set_title('Server Summary')
                ax4.axis('off')
                
                plt.tight_layout()
                
                # Convert to Discord file
                file = await self._fig_to_discord_file(fig, "server_overview.png")
                
                # Cache the result
                buf = io.BytesIO()
                fig.savefig(buf, format='png', dpi=PLOT_DPI, bbox_inches='tight')
                self._cache.set(cache_key, buf.getvalue())
                
                return file
                
            except Exception as e:
                self.logger.error(f"Error creating server overview plot: {e}")
                return await self._create_error_plot("Error creating server overview")
    
    @memory_safe_plot
    async def plot_godpack_analysis(self, days_back: int = 30) -> discord.File:
        """Create god pack analysis plots"""
        async with self._plot_semaphore:
            # Check cache
            cache_key = f"godpack_analysis_{days_back}"
            cached_data = self._cache.get(cache_key)
            if cached_data:
                return discord.File(io.BytesIO(cached_data), filename="godpack_analysis.png")
            
            try:
                # Get god pack data
                godpacks = self.db.get_all_godpacks(limit=100)  # Get recent god packs
                
                if not godpacks:
                    return await self._create_no_data_plot("No god pack data available")
                
                # Create figure
                fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=PLOT_FIGURE_SIZE)
                
                # Plot 1: God Packs Over Time (by day)
                dates = [gp.timestamp.date() for gp in godpacks]
                unique_dates = sorted(set(dates))
                date_counts = [dates.count(d) for d in unique_dates]
                
                ax1.bar(unique_dates, date_counts, alpha=0.7, color='purple')
                ax1.set_title('God Packs Found Over Time')
                ax1.set_xlabel('Date')
                ax1.set_ylabel('Number of God Packs')
                plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
                
                # Plot 2: State Distribution
                states = [gp.state.value for gp in godpacks]
                state_counts = {}
                for state in states:
                    state_counts[state] = state_counts.get(state, 0) + 1
                
                if state_counts:
                    ax2.pie(state_counts.values(), labels=state_counts.keys(), autopct='%1.1f%%')
                    ax2.set_title('God Pack State Distribution')
                
                # Plot 3: Pack Name Distribution
                names = [gp.name for gp in godpacks]
                name_counts = {}
                for name in names:
                    name_counts[name] = name_counts.get(name, 0) + 1
                
                # Top 5 most common
                top_names = sorted(name_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                if top_names:
                    names_list, counts_list = zip(*top_names)
                    ax3.bar(names_list, counts_list, alpha=0.7, color='orange')
                    ax3.set_title('Top God Pack Types')
                    ax3.set_ylabel('Count')
                    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
                
                # Plot 4: Summary Stats
                total_gps = len(godpacks)
                alive_gps = len([gp for gp in godpacks if gp.state.value in ['ALIVE', 'TESTING']])
                success_rate = (alive_gps / total_gps * 100) if total_gps > 0 else 0
                
                ax4.text(0.5, 0.7, f"Total God Packs: {total_gps}", ha='center', fontsize=12)
                ax4.text(0.5, 0.5, f"Currently Alive: {alive_gps}", ha='center', fontsize=12)
                ax4.text(0.5, 0.3, f"Success Rate: {success_rate:.1f}%", ha='center', fontsize=12)
                ax4.set_xlim(0, 1)
                ax4.set_ylim(0, 1)
                ax4.set_title('God Pack Summary')
                ax4.axis('off')
                
                plt.tight_layout()
                
                # Convert to Discord file
                file = await self._fig_to_discord_file(fig, "godpack_analysis.png")
                
                # Cache the result
                buf = io.BytesIO()
                fig.savefig(buf, format='png', dpi=PLOT_DPI, bbox_inches='tight')
                self._cache.set(cache_key, buf.getvalue())
                
                return file
                
            except Exception as e:
                self.logger.error(f"Error creating god pack analysis: {e}")
                return await self._create_error_plot("Error creating god pack analysis")
    
    @memory_safe_plot
    async def plot_probability_trends(self, gp_id: int) -> discord.File:
        """Generate probability trend plot for a god pack"""
        async with self._plot_semaphore:
            try:
                # Get test results
                test_results = self.db.get_test_results(gp_id)
                godpack = self.db.get_godpack(gp_id=gp_id)
                
                if not test_results or not godpack:
                    return await self._create_no_data_plot(f"No test data for GP {gp_id}")
                
                # Create figure
                fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
                
                # Plot 1: Simple probability decline
                test_count = list(range(len(test_results) + 1))
                # Simple probability calculation: start at 100%, decrease by 15% per miss, 5% per noshow
                probabilities = [100.0]
                current_prob = 100.0
                
                for test in test_results:
                    if test.test_type.value == 'MISS':
                        current_prob = max(0, current_prob - 15)
                    elif test.test_type.value == 'NOSHOW':
                        current_prob = max(0, current_prob - 5)
                    probabilities.append(current_prob)
                
                ax1.plot(test_count, probabilities, 'r-', linewidth=3, marker='o', markersize=6)
                ax1.set_title(f'Probability Trend - GP {gp_id} ({godpack.name})')
                ax1.set_xlabel('Test Number')
                ax1.set_ylabel('Probability Alive (%)')
                ax1.grid(True, alpha=0.3)
                ax1.set_ylim(0, 105)
                
                # Add probability zones
                ax1.axhspan(80, 100, alpha=0.2, color='green', label='High (80%+)')
                ax1.axhspan(50, 80, alpha=0.2, color='yellow', label='Medium (50-80%)')
                ax1.axhspan(20, 50, alpha=0.2, color='orange', label='Low (20-50%)')
                ax1.axhspan(0, 20, alpha=0.2, color='red', label='Very Low (<20%)')
                ax1.legend(loc='upper right')
                
                # Plot 2: Test Timeline
                if test_results:
                    test_types = [test.test_type.value for test in test_results]
                    test_times = [test.timestamp for test in test_results]
                    
                    colors = ['red' if t == 'MISS' else 'orange' for t in test_types]
                    y_positions = list(range(len(test_times)))
                    
                    ax2.scatter(test_times, y_positions, c=colors, s=100, alpha=0.7)
                    
                    for i, (time, test_type) in enumerate(zip(test_times, test_types)):
                        ax2.annotate(test_type, (time, i), xytext=(5, 0), textcoords='offset points')
                    
                    ax2.set_title('Test Timeline')
                    ax2.set_xlabel('Time')
                    ax2.set_ylabel('Test Number')
                    ax2.grid(True, alpha=0.3)
                
                plt.tight_layout()
                
                # Convert to Discord file
                return await self._fig_to_discord_file(fig, f"probability_trends_{gp_id}.png")
                
            except Exception as e:
                self.logger.error(f"Error creating probability trends plot: {e}")
                return await self._create_error_plot("Error creating probability trends")
    
    @memory_safe_plot
    async def _create_no_data_plot(self, message: str) -> discord.File:
        """Create a simple plot indicating no data available"""
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, message, ha='center', va='center', fontsize=16,
                bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgray", alpha=0.8))
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        
        return await self._fig_to_discord_file(fig, "no_data.png")
    
    @memory_safe_plot
    async def _create_error_plot(self, message: str) -> discord.File:
        """Create a simple plot indicating an error occurred"""
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, f"❌ {message}", ha='center', va='center', fontsize=16, color='red',
                bbox=dict(boxstyle="round,pad=0.5", facecolor="mistyrose", alpha=0.8))
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        
        return await self._fig_to_discord_file(fig, "error.png")
    
    def clear_cache(self):
        """Clear the plot cache"""
        self._cache.clear_expired()
        self.logger.info("Cleared expired plot cache entries")
    
    async def cleanup(self):
        """Cleanup resources"""
        plt.close('all')
        gc.collect()
        self.clear_cache()
        self.logger.info("Plotting system cleaned up")