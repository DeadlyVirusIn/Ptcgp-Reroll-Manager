import math
import sqlite3
from typing import List, Dict, Tuple
from datetime import datetime
import logging
from dataclasses import dataclass
from database_manager import TestType

@dataclass
class ProbabilityResult:
    gp_id: int
    probability_alive: float
    total_tests: int
    miss_tests: int
    noshow_tests: int
    confidence_level: float
    member_probabilities: Dict[int, float]
    last_calculated: datetime

class ProbabilityCalculator:
    def __init__(self, db_manager):
        self.db = db_manager
        self.logger = logging.getLogger(__name__)

    def calculate_noshow_probability(self, open_slots: int, number_friends: int) -> float:
        """
        Calculate the probability that a no-show is equivalent to a dud.
        Based on hypergeometric distribution.
        """
        if number_friends < 6:
            number_friends = 6  # Minimum friends assumption
        
        if open_slots < 0 or number_friends < 0:
            return 1.0  # Conservative estimate
        
        if open_slots >= number_friends:
            return 1.0  # If more slots than friends, definitely a dud
        
        # Check if mathematically possible
        if number_friends - (4 - open_slots) - 1 < open_slots:
            return 1.0
        
        try:
            # Calculate using combinations
            numerator = math.comb(number_friends - (4 - open_slots) - 1, open_slots)
            denominator = math.comb(number_friends - (4 - open_slots), open_slots)
            
            if denominator == 0:
                return 1.0
            
            probability = 1.0 - (numerator / denominator)
            return max(0.0, min(1.0, probability))  # Clamp between 0 and 1
        
        except (ValueError, OverflowError):
            return 1.0  # Conservative fallback

    def calculate_godpack_probability(self, gp_id: int, force_recalculate: bool = False) -> ProbabilityResult:
        """
        Calculate the probability that a god pack is still alive based on test results.
        Uses Bayesian updating with each test result.
        """
        godpack = self.db.get_godpack(gp_id=gp_id)
        if not godpack:
            raise ValueError(f"God pack {gp_id} not found")

        # Check if we have cached results and don't need to recalculate
        if not force_recalculate:
            cached = self._get_cached_probability(gp_id)
            if cached and (datetime.now() - cached.last_calculated).seconds < 300:  # 5 minute cache
                return cached

        test_results = self.db.get_test_results(gp_id)
        
        if not test_results:
            # No tests yet, assume 100% probability
            result = ProbabilityResult(
                gp_id=gp_id,
                probability_alive=100.0,
                total_tests=0,
                miss_tests=0,
                noshow_tests=0,
                confidence_level=0.0,
                member_probabilities={},
                last_calculated=datetime.now()
            )
            self._cache_probability(result)
            return result

        # Group tests by member
        member_tests = {}
        for test in test_results:
            if test.discord_id not in member_tests:
                member_tests[test.discord_id] = []
            member_tests[test.discord_id].append(test)

        # Calculate probability for each member and combine
        overall_probability = 1.0
        member_probabilities = {}
        total_miss = 0
        total_noshow = 0

        for member_id, tests in member_tests.items():
            member_prob = self._calculate_member_probability(godpack, tests)
            member_probabilities[member_id] = member_prob
            overall_probability *= member_prob
            
            # Count test types
            for test in tests:
                if test.test_type == TestType.MISS:
                    total_miss += 1
                elif test.test_type == TestType.NOSHOW:
                    total_noshow += 1

        # Convert to percentage
        probability_percentage = overall_probability * 100.0

        # Calculate confidence level based on number of tests
        confidence_level = self._calculate_confidence_level(len(test_results), total_miss, total_noshow)

        result = ProbabilityResult(
            gp_id=gp_id,
            probability_alive=probability_percentage,
            total_tests=len(test_results),
            miss_tests=total_miss,
            noshow_tests=total_noshow,
            confidence_level=confidence_level,
            member_probabilities=member_probabilities,
            last_calculated=datetime.now()
        )

        self._cache_probability(result)
        return result

    def _calculate_member_probability(self, godpack, tests: List) -> float:
        if not godpack or not tests:
        return 0.0
        """
        Calculate the probability contribution from a single member's tests.
        Each test reduces the probability based on the Bayesian model.
        """
        # Start with the base probability based on pack number
        base_probability = min(godpack.pack_number, 5) / 5.0  # Normalize to 0-1
        
        if base_probability <= 0:
            return 0.0

        current_packs_remaining = base_probability * 5  # Convert back to pack count for calculation
        
        for test in tests:
            if current_packs_remaining <= 0:
                return 0.0
            
            if test.test_type == TestType.MISS:
                # A miss test removes exactly 1 pack equivalent
                duds_equivalent = 1.0
            elif test.test_type == TestType.NOSHOW:
                # A no-show test removes based on probability calculation
                duds_equivalent = self.calculate_noshow_probability(
                    test.open_slots, test.number_friends
                )
            else:
                continue
            
            # Update remaining packs (probability)
            current_packs_remaining = max(0, current_packs_remaining - duds_equivalent)
        
        # Return the probability (0-1 scale)
        return current_packs_remaining / max(godpack.pack_number, 1)

    def _calculate_confidence_level(self, total_tests: int, miss_tests: int, noshow_tests: int) -> float:
        """
        Calculate confidence level based on the quantity and quality of tests.
        More tests = higher confidence, miss tests = higher confidence than no-shows
        """
        if total_tests == 0:
            return 0.0
        
        # Weight miss tests more heavily than no-show tests
        weighted_tests = miss_tests * 1.0 + noshow_tests * 0.7
        
        # Confidence increases with number of tests but with diminishing returns
        confidence = (1 - math.exp(-weighted_tests / 3)) * 100
        
        return min(confidence, 95.0)  # Cap at 95%

    def _get_cached_probability(self, gp_id: int) -> ProbabilityResult:
        """Get cached probability calculation"""
        with self.db.lock:
            conn = sqlite3.connect(self.db.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM gp_statistics WHERE gp_id = ?
            ''', (gp_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return ProbabilityResult(
                    gp_id=gp_id,
                    probability_alive=row['probability_alive'],
                    total_tests=row['total_tests'],
                    miss_tests=row['miss_tests'],
                    noshow_tests=row['noshow_tests'],
                    confidence_level=row.get('confidence_level', 0.0),
                    member_probabilities={},  # Not cached
                    last_calculated=datetime.fromisoformat(row['last_calculated'])
                )
            return None

    def _cache_probability(self, result: ProbabilityResult):
        """Cache probability calculation"""
        with self.db.lock:
            conn = sqlite3.connect(self.db.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO gp_statistics 
                (gp_id, probability_alive, total_tests, miss_tests, noshow_tests, 
                 confidence_level, last_calculated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (result.gp_id, result.probability_alive, result.total_tests,
                  result.miss_tests, result.noshow_tests, result.confidence_level,
                  result.last_calculated))
            
            conn.commit()
            conn.close()

    def add_test_and_calculate(self, discord_id: int, gp_id: int, test_type: TestType,
                              open_slots: int = -1, number_friends: int = -1) -> ProbabilityResult:
        """
        Add a new test result and immediately calculate updated probability
        """
        # Add the test result
        self.db.add_test_result(discord_id, gp_id, test_type, open_slots, number_friends)
        
        # Calculate and return updated probability
        return self.calculate_godpack_probability(gp_id, force_recalculate=True)

    def get_probability_summary(self, gp_id: int) -> Dict:
        """
        Get a comprehensive summary of probability calculation for display
        """
        result = self.calculate_godpack_probability(gp_id)
        godpack = self.db.get_godpack(gp_id=gp_id)
        test_results = self.db.get_test_results(gp_id)
        
        # Get member names for display
        member_details = {}
        for member_id, prob in result.member_probabilities.items():
            user = self.db.get_user(member_id)
            member_details[member_id] = {
                'name': user['display_name'] if user else f"User {member_id}",
                'probability': prob * 100,
                'tests': [t for t in test_results if t.discord_id == member_id]
            }
        
        return {
            'godpack': godpack,
            'probability': result.probability_alive,
            'confidence': result.confidence_level,
            'total_tests': result.total_tests,
            'breakdown': {
                'miss_tests': result.miss_tests,
                'noshow_tests': result.noshow_tests
            },
            'member_details': member_details,
            'recommendation': self._get_recommendation(result)
        }

    def _get_recommendation(self, result: ProbabilityResult) -> str:
        """
        Generate a recommendation based on probability and confidence
        """
        prob = result.probability_alive
        conf = result.confidence_level
        
        if conf < 30:
            return "⚠️ More tests needed for reliable assessment"
        elif prob > 80 and conf > 50:
            return "✅ Likely ALIVE - High confidence"
        elif prob > 60 and conf > 40:
            return "🟡 Possibly ALIVE - Moderate confidence"
        elif prob > 30 and conf > 50:
            return "🟠 Uncertain - Consider more testing"
        elif prob < 30 and conf > 60:
            return "❌ Likely DEAD - High confidence"
        else:
            return "❓ Inconclusive - More testing recommended"

    def bulk_recalculate_probabilities(self, states_to_recalc: List[str]):
        """Recalculate probabilities for god packs in specified states"""
        try:
            with self.db.lock:
                conn = sqlite3.connect(self.db.db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Get godpacks that need recalculation
                placeholders = ','.join(['?' for _ in states_to_recalc])
                cursor.execute(f'''
                    SELECT id FROM godpacks 
                    WHERE state IN ({placeholders})
                ''', states_to_recalc)
                
                gp_ids = [row['id'] for row in cursor.fetchall()]
                conn.close()
            
            recalc_count = 0
            for gp_id in gp_ids:
                try:
                    self.calculate_godpack_probability(gp_id, force_recalculate=True)
                    recalc_count += 1
                except Exception as e:
                    self.logger.error(f"Error recalculating probability for GP {gp_id}: {e}")
            
            self.logger.info(f"Recalculated probabilities for {recalc_count} god packs")
            
        except Exception as e:
            self.logger.error(f"Error in bulk recalculation: {e}")

    def get_all_probabilities(self, min_confidence: float = 0.0) -> List[Dict]:
        """
        Get probability summaries for all god packs with optional confidence filter
        """
        with self.db.lock:
            conn = sqlite3.connect(self.db.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT id FROM godpacks 
                WHERE state IN ('TESTING', 'ALIVE')
                ORDER BY timestamp DESC
            ''')
            
            gp_ids = [row['id'] for row in cursor.fetchall()]
            conn.close()
        
        results = []
        for gp_id in gp_ids:
            try:
                result = self.calculate_godpack_probability(gp_id)
                if result.confidence_level >= min_confidence:
                    results.append({
                        'gp_id': gp_id,
                        'probability': result.probability_alive,
                        'confidence': result.confidence_level,
                        'total_tests': result.total_tests,
                        'recommendation': self._get_recommendation(result)
                    })
            except Exception as e:
                self.logger.error(f"Error calculating probability for GP {gp_id}: {e}")
        
        return results

    def get_probability_statistics(self) -> Dict:
        """
        Get overall statistics about probability calculations
        """
        with self.db.lock:
            conn = sqlite3.connect(self.db.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get overall statistics
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_gps,
                    AVG(probability_alive) as avg_probability,
                    AVG(confidence_level) as avg_confidence,
                    AVG(total_tests) as avg_tests_per_gp,
                    SUM(miss_tests) as total_miss_tests,
                    SUM(noshow_tests) as total_noshow_tests
                FROM gp_statistics gs
                JOIN godpacks g ON gs.gp_id = g.id
                WHERE g.state IN ('TESTING', 'ALIVE')
            ''')
            
            stats = cursor.fetchone()
            
            # Get probability distribution
            cursor.execute('''
                SELECT 
                    CASE 
                        WHEN probability_alive >= 80 THEN 'High (80%+)'
                        WHEN probability_alive >= 50 THEN 'Medium (50-79%)'
                        WHEN probability_alive >= 20 THEN 'Low (20-49%)'
                        ELSE 'Very Low (<20%)'
                    END as probability_range,
                    COUNT(*) as count
                FROM gp_statistics gs
                JOIN godpacks g ON gs.gp_id = g.id
                WHERE g.state IN ('TESTING', 'ALIVE')
                GROUP BY 
                    CASE 
                        WHEN probability_alive >= 80 THEN 'High (80%+)'
                        WHEN probability_alive >= 50 THEN 'Medium (50-79%)'
                        WHEN probability_alive >= 20 THEN 'Low (20-49%)'
                        ELSE 'Very Low (<20%)'
                    END
            ''')
            
            distribution = cursor.fetchall()
            conn.close()
        
        return {
            'total_godpacks': stats['total_gps'] or 0,
            'average_probability': stats['avg_probability'] or 0,
            'average_confidence': stats['avg_confidence'] or 0,
            'average_tests_per_gp': stats['avg_tests_per_gp'] or 0,
            'total_miss_tests': stats['total_miss_tests'] or 0,
            'total_noshow_tests': stats['total_noshow_tests'] or 0,
            'probability_distribution': {row['probability_range']: row['count'] for row in distribution}
        }