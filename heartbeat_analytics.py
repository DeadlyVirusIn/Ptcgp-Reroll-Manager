import sqlite3
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import logging

@dataclass
class HeartbeatRun:
    """Represents a continuous run of heartbeats from a user"""
    discord_id: int
    start_time: datetime
    end_time: datetime
    start_packs: int
    end_packs: int
    total_packs: int
    duration_minutes: float
    average_instances: float
    peak_instances: int
    packs_per_minute: float
    main_instance_time: float  # Percentage of time main was running

@dataclass
class UserStats:
    """Comprehensive user statistics"""
    discord_id: int
    display_name: str
    total_runs: int
    total_runtime_hours: float
    total_packs: int
    average_packs_per_minute: float
    average_instances: float
    peak_instances: int
    efficiency_score: float  # Packs per instance-hour
    consistency_score: float  # How consistent their performance is
    last_active: datetime
    status: str

@dataclass
class ServerStats:
    """Server-wide statistics"""
    active_users: int
    total_instances: int
    total_packs_per_hour: float
    average_efficiency: float
    top_performers: List[Dict]
    timeline_data: Dict

class HeartbeatAnalytics:
    def __init__(self, db_manager):
        self.db = db_manager
        self.logger = logging.getLogger(__name__)

    def detect_runs(self, discord_id: int, days_back: int = 7, 
                   gap_threshold_minutes: int = 60) -> List[HeartbeatRun]:
        """
        Detect continuous runs of activity from heartbeats.
        A run ends when there's a gap larger than gap_threshold_minutes.
        """
        heartbeats = self.db.get_heartbeats_for_user(discord_id, days_back)
        
        if not heartbeats:
            return []

        runs = []
        current_run_heartbeats = []
        
        for heartbeat in heartbeats:
            if not current_run_heartbeats:
                current_run_heartbeats = [heartbeat]
                continue
            
            # Check time gap from last heartbeat
            time_gap = (heartbeat.timestamp - current_run_heartbeats[-1].timestamp).total_seconds() / 60
            
            if time_gap > gap_threshold_minutes:
                # End current run and start new one
                if len(current_run_heartbeats) > 1:  # Only process runs with multiple heartbeats
                    run = self._create_run_from_heartbeats(current_run_heartbeats)
                    if run:
                        runs.append(run)
                current_run_heartbeats = [heartbeat]
            else:
                current_run_heartbeats.append(heartbeat)
        
        # Process final run
        if len(current_run_heartbeats) > 1:
            run = self._create_run_from_heartbeats(current_run_heartbeats)
            if run:
                runs.append(run)
        
        return runs

    def _create_run_from_heartbeats(self, heartbeats: List) -> Optional[HeartbeatRun]:
        """Create a HeartbeatRun object from a list of heartbeats"""
        if len(heartbeats) < 2:
            return None
        
        start_hb = heartbeats[0]
        end_hb = heartbeats[-1]
        
        # Calculate duration
        duration = (end_hb.timestamp - start_hb.timestamp).total_seconds() / 60  # minutes
        if duration <= 0:
            return None
        
        # Calculate statistics
        total_packs = end_hb.packs - start_hb.packs
        instances = [hb.instances_online + hb.instances_offline for hb in heartbeats]
        average_instances = np.mean(instances)
        peak_instances = max(instances)
        
        # Calculate main instance time (percentage of heartbeats with main on)
        main_on_count = sum(1 for hb in heartbeats if hb.main_on)
        main_instance_time = (main_on_count / len(heartbeats)) * 100
        
        # Calculate packs per minute
        packs_per_minute = total_packs / duration if duration > 0 else 0
        
        return HeartbeatRun(
            discord_id=start_hb.discord_id,
            start_time=start_hb.timestamp,
            end_time=end_hb.timestamp,
            start_packs=start_hb.packs,
            end_packs=end_hb.packs,
            total_packs=total_packs,
            duration_minutes=duration,
            average_instances=average_instances,
            peak_instances=peak_instances,
            packs_per_minute=packs_per_minute,
            main_instance_time=main_instance_time
        )

    def get_user_statistics(self, discord_id: int, days_back: int = 30) -> UserStats:
        """Get comprehensive statistics for a user"""
        runs = self.detect_runs(discord_id, days_back)
        user_data = self.db.get_user(discord_id)
        
        if not user_data:
            raise ValueError(f"User {discord_id} not found")
        
        if not runs:
            return UserStats(
                discord_id=discord_id,
                display_name=user_data.get('display_name', f'User {discord_id}'),
                total_runs=0,
                total_runtime_hours=0,
                total_packs=0,
                average_packs_per_minute=0,
                average_instances=0,
                peak_instances=0,
                efficiency_score=0,
                consistency_score=0,
                last_active=datetime.fromisoformat(user_data.get('last_heartbeat')) if user_data.get('last_heartbeat') else datetime.min,
                status=user_data.get('status', 'inactive')
            )
        
        # Calculate aggregated statistics
        total_runtime_minutes = sum(run.duration_minutes for run in runs)
        total_runtime_hours = total_runtime_minutes / 60
        total_packs = sum(run.total_packs for run in runs)
        
        # Calculate averages
        avg_packs_per_minute = np.mean([run.packs_per_minute for run in runs])
        avg_instances = np.mean([run.average_instances for run in runs])
        peak_instances = max(run.peak_instances for run in runs)
        
        # Calculate efficiency (packs per instance-hour)
        total_instance_hours = sum(run.average_instances * run.duration_minutes / 60 for run in runs)
        efficiency_score = total_packs / total_instance_hours if total_instance_hours > 0 else 0
        
        # Calculate consistency (lower coefficient of variation = higher consistency)
        if len(runs) > 1:
            ppm_values = [run.packs_per_minute for run in runs if run.packs_per_minute > 0]
            if ppm_values:
                cv = np.std(ppm_values) / np.mean(ppm_values)
                consistency_score = max(0, 100 - cv * 100)  # Convert to 0-100 scale
            else:
                consistency_score = 0
        else:
            consistency_score = 50  # Neutral for single run
        
        return UserStats(
            discord_id=discord_id,
            display_name=user_data.get('display_name', f'User {discord_id}'),
            total_runs=len(runs),
            total_runtime_hours=total_runtime_hours,
            total_packs=total_packs,
            average_packs_per_minute=avg_packs_per_minute,
            average_instances=avg_instances,
            peak_instances=peak_instances,
            efficiency_score=efficiency_score,
            consistency_score=consistency_score,
            last_active=max(run.end_time for run in runs),
            status=user_data.get('status', 'inactive')
        )

    def get_server_statistics(self, days_back: int = 7) -> ServerStats:
        """Get server-wide statistics"""
        active_users = self.db.get_active_users(minutes_back=60)  # Active in last hour
        
        # Get all user statistics
        all_user_stats = []
        total_instances = 0
        total_packs_hour = 0
        
        for user in active_users:
            try:
                stats = self.get_user_statistics(user['discord_id'], days_back)
                all_user_stats.append(stats)
                
                # For currently active users, get their current instance count
                recent_heartbeats = self.db.get_heartbeats_for_user(user['discord_id'], days_back=1)
                if recent_heartbeats:
                    latest_hb = recent_heartbeats[-1]
                    total_instances += latest_hb.instances_online + latest_hb.instances_offline
                    
                    # Calculate current packs per hour rate
                    if len(recent_heartbeats) >= 2:
                        recent_ppm = stats.average_packs_per_minute
                        total_packs_hour += recent_ppm * 60
                        
            except Exception as e:
                self.logger.error(f"Error calculating stats for user {user['discord_id']}: {e}")
        
        # Calculate average efficiency
        efficiencies = [stats.efficiency_score for stats in all_user_stats if stats.efficiency_score > 0]
        average_efficiency = np.mean(efficiencies) if efficiencies else 0
        
        # Get top performers
        top_performers = sorted(all_user_stats, 
                               key=lambda x: x.efficiency_score, 
                               reverse=True)[:5]
        
        top_performers_data = [
            {
                'name': stats.display_name,
                'efficiency': stats.efficiency_score,
                'total_packs': stats.total_packs,
                'runtime_hours': stats.total_runtime_hours
            }
            for stats in top_performers
        ]
        
        # Generate timeline data
        timeline_data = self._generate_timeline_data(days_back)
        
        return ServerStats(
            active_users=len(active_users),
            total_instances=total_instances,
            total_packs_per_hour=total_packs_hour,
            average_efficiency=average_efficiency,
            top_performers=top_performers_data,
            timeline_data=timeline_data
        )

    def _generate_timeline_data(self, days_back: int) -> Dict:
        """Generate timeline data for plotting"""
        with self.db.lock:
            conn = sqlite3.connect(self.db.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get hourly aggregated data
            since_date = datetime.now() - timedelta(days=days_back)
            
            cursor.execute('''
                SELECT 
                    datetime(timestamp, 'start of hour') as hour,
                    COUNT(DISTINCT discord_id) as active_users,
                    AVG(instances_online + instances_offline) as avg_instances,
                    SUM(CASE WHEN main_on THEN 1 ELSE 0 END) as main_instances,
                    COUNT(*) as heartbeat_count
                FROM heartbeats 
                WHERE timestamp >= ?
                GROUP BY datetime(timestamp, 'start of hour')
                ORDER BY hour
            ''', (since_date,))
            
            rows = cursor.fetchall()
            conn.close()
        
        timeline = {
            'timestamps': [],
            'active_users': [],
            'avg_instances': [],
            'main_instances': [],
            'heartbeat_count': []
        }
        
        for row in rows:
            timeline['timestamps'].append(row['hour'])
            timeline['active_users'].append(row['active_users'])
            timeline['avg_instances'].append(row['avg_instances'] or 0)
            timeline['main_instances'].append(row['main_instances'])
            timeline['heartbeat_count'].append(row['heartbeat_count'])
        
        return timeline

    def detect_anomalies(self, discord_id: int, days_back: int = 7) -> List[Dict]:
        """
        Detect anomalies in user behavior (unusual spikes, drops, etc.)
        """
        runs = self.detect_runs(discord_id, days_back)
        
        if len(runs) < 3:  # Need enough data for anomaly detection
            return []
        
        anomalies = []
        
        # Calculate statistical thresholds
        ppm_values = [run.packs_per_minute for run in runs]
        instance_values = [run.average_instances for run in runs]
        
        ppm_mean = np.mean(ppm_values)
        ppm_std = np.std(ppm_values)
        instance_mean = np.mean(instance_values)
        instance_std = np.std(instance_values)
        
        # Define thresholds (2 standard deviations)
        ppm_threshold_high = ppm_mean + 2 * ppm_std
        ppm_threshold_low = max(0, ppm_mean - 2 * ppm_std)
        instance_threshold_high = instance_mean + 2 * instance_std
        
        for run in runs:
            # Check for performance anomalies
            if run.packs_per_minute > ppm_threshold_high:
                anomalies.append({
                    'type': 'high_performance',
                    'timestamp': run.start_time,
                    'value': run.packs_per_minute,
                    'threshold': ppm_threshold_high,
                    'description': f'Unusually high packs/min: {run.packs_per_minute:.2f}'
                })
            
            if run.packs_per_minute < ppm_threshold_low and run.packs_per_minute > 0:
                anomalies.append({
                    'type': 'low_performance',
                    'timestamp': run.start_time,
                    'value': run.packs_per_minute,
                    'threshold': ppm_threshold_low,
                    'description': f'Unusually low packs/min: {run.packs_per_minute:.2f}'
                })
            
            # Check for instance count anomalies
            if run.average_instances > instance_threshold_high:
                anomalies.append({
                    'type': 'high_instances',
                    'timestamp': run.start_time,
                    'value': run.average_instances,
                    'threshold': instance_threshold_high,
                    'description': f'Unusually high instances: {run.average_instances:.1f}'
                })
            
            # Check for very long runs (potential botting detection)
            if run.duration_minutes > 8 * 60:  # 8 hours
                anomalies.append({
                    'type': 'long_session',
                    'timestamp': run.start_time,
                    'value': run.duration_minutes,
                    'threshold': 8 * 60,
                    'description': f'Very long session: {run.duration_minutes/60:.1f} hours'
                })
        
        return sorted(anomalies, key=lambda x: x['timestamp'], reverse=True)

    def generate_leaderboard(self, metric: str = 'efficiency', days_back: int = 7, 
                           limit: int = 10) -> List[Dict]:
        """
        Generate leaderboard based on specified metric
        """
        active_users = self.db.get_active_users(minutes_back=days_back * 24 * 60)
        user_stats = []
        
        for user in active_users:
            try:
                stats = self.get_user_statistics(user['discord_id'], days_back)
                if stats.total_packs > 0:  # Only include users with activity
                    user_stats.append(stats)
            except Exception as e:
                self.logger.error(f"Error calculating stats for leaderboard: {e}")
        
        # Sort based on metric
        if metric == 'efficiency':
            sorted_stats = sorted(user_stats, key=lambda x: x.efficiency_score, reverse=True)
        elif metric == 'total_packs':
            sorted_stats = sorted(user_stats, key=lambda x: x.total_packs, reverse=True)
        elif metric == 'runtime':
            sorted_stats = sorted(user_stats, key=lambda x: x.total_runtime_hours, reverse=True)
        elif metric == 'consistency':
            sorted_stats = sorted(user_stats, key=lambda x: x.consistency_score, reverse=True)
        else:
            sorted_stats = user_stats
        
        leaderboard = []
        for i, stats in enumerate(sorted_stats[:limit]):
            leaderboard.append({
                'rank': i + 1,
                'name': stats.display_name,
                'value': getattr(stats, metric),
                'total_packs': stats.total_packs,
                'runtime_hours': stats.total_runtime_hours,
                'efficiency': stats.efficiency_score,
                'consistency': stats.consistency_score
            })
        
        return leaderboard

    def cache_run_data(self):
        """Cache run data in the database for faster future queries"""
        active_users = self.db.get_active_users(minutes_back=7 * 24 * 60)  # Last week
        cached_count = 0
        
        with self.db.lock:
            conn = sqlite3.connect(self.db.db_path)
            cursor = conn.cursor()
            
            for user in active_users:
                try:
                    runs = self.detect_runs(user['discord_id'], days_back=7)
                    
                    for run in runs:
                        cursor.execute('''
                            INSERT OR REPLACE INTO heartbeat_runs 
                            (discord_id, start_time, end_time, start_packs, end_packs, average_instances)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (run.discord_id, run.start_time, run.end_time, 
                              run.start_packs, run.end_packs, run.average_instances))
                        cached_count += 1
                        
                except Exception as e:
                    self.logger.error(f"Error caching runs for user {user['discord_id']}: {e}")
            
            conn.commit()
            conn.close()
        
        self.logger.info(f"Cached {cached_count} heartbeat runs")
        return cached_count