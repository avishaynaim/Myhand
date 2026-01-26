"""
Analytics Module for Yad2 Monitor
Price trends, market insights, time-on-market tracking
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import statistics
import logging

logger = logging.getLogger(__name__)


class MarketAnalytics:
    """Analyzes market data and provides insights"""

    def __init__(self, database):
        self.db = database

    def get_price_trends(self, days: int = 30, group_by: str = 'neighborhood') -> Dict:
        """
        Calculate price trends over time.
        group_by: 'neighborhood', 'city', or 'all'
        """
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            cutoff = (datetime.now() - timedelta(days=days)).isoformat()

            # Get price history with apartment details
            cursor.execute('''
                SELECT a.neighborhood, a.city, a.rooms,
                       ph.price, ph.recorded_at,
                       a.first_seen
                FROM price_history ph
                JOIN apartments a ON ph.apartment_id = a.id
                WHERE ph.recorded_at > ?
                ORDER BY ph.recorded_at
            ''', (cutoff,))

            rows = cursor.fetchall()

            if not rows:
                return {'message': 'Not enough data for trend analysis'}

            # Group by specified field
            if group_by == 'neighborhood':
                groups = defaultdict(list)
                for row in rows:
                    if row['neighborhood']:
                        groups[row['neighborhood']].append({
                            'price': row['price'],
                            'date': row['recorded_at'][:10]
                        })
            elif group_by == 'city':
                groups = defaultdict(list)
                for row in rows:
                    if row['city']:
                        groups[row['city']].append({
                            'price': row['price'],
                            'date': row['recorded_at'][:10]
                        })
            else:
                groups = {'all': [{'price': row['price'], 'date': row['recorded_at'][:10]} for row in rows]}

            # Calculate trends for each group
            trends = {}
            for group_name, prices in groups.items():
                if len(prices) < 2:
                    continue

                # Group by date
                by_date = defaultdict(list)
                for p in prices:
                    by_date[p['date']].append(p['price'])

                # Calculate daily averages
                daily_avgs = {date: statistics.mean(prices) for date, prices in sorted(by_date.items())}

                if len(daily_avgs) < 2:
                    continue

                dates = list(daily_avgs.keys())
                first_avg = daily_avgs[dates[0]]
                last_avg = daily_avgs[dates[-1]]

                change = last_avg - first_avg
                change_pct = (change / first_avg) * 100 if first_avg > 0 else 0

                trends[group_name] = {
                    'first_date': dates[0],
                    'last_date': dates[-1],
                    'first_avg_price': round(first_avg),
                    'last_avg_price': round(last_avg),
                    'change': round(change),
                    'change_pct': round(change_pct, 1),
                    'direction': 'up' if change > 0 else 'down' if change < 0 else 'stable',
                    'sample_size': len(prices)
                }

            return {
                'period_days': days,
                'group_by': group_by,
                'trends': trends,
                'generated_at': datetime.now().isoformat()
            }

    def get_market_insights(self) -> Dict:
        """Generate market insights and statistics"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            insights = {}

            # Overall stats
            cursor.execute('''
                SELECT COUNT(*) as total,
                       AVG(price) as avg_price,
                       MIN(price) as min_price,
                       MAX(price) as max_price,
                       AVG(rooms) as avg_rooms,
                       AVG(sqm) as avg_sqm
                FROM apartments WHERE is_active = 1
            ''')
            overall = cursor.fetchone()
            insights['overall'] = {
                'total_listings': overall['total'],
                'avg_price': round(overall['avg_price']) if overall['avg_price'] else 0,
                'min_price': overall['min_price'],
                'max_price': overall['max_price'],
                'avg_rooms': round(overall['avg_rooms'], 1) if overall['avg_rooms'] else 0,
                'avg_sqm': round(overall['avg_sqm']) if overall['avg_sqm'] else 0
            }

            # Price per sqm analysis
            cursor.execute('''
                SELECT price, sqm, neighborhood, city
                FROM apartments
                WHERE is_active = 1 AND price > 0 AND sqm > 0
            ''')
            sqm_data = cursor.fetchall()

            if sqm_data:
                price_per_sqm = [row['price'] / row['sqm'] for row in sqm_data]
                insights['price_per_sqm'] = {
                    'avg': round(statistics.mean(price_per_sqm)),
                    'median': round(statistics.median(price_per_sqm)),
                    'min': round(min(price_per_sqm)),
                    'max': round(max(price_per_sqm))
                }

                # By neighborhood
                by_neighborhood = defaultdict(list)
                for row in sqm_data:
                    if row['neighborhood']:
                        by_neighborhood[row['neighborhood']].append(row['price'] / row['sqm'])

                insights['price_per_sqm_by_neighborhood'] = {
                    n: round(statistics.mean(prices))
                    for n, prices in by_neighborhood.items()
                    if len(prices) >= 3
                }

            # New listings this week
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            cursor.execute('''
                SELECT COUNT(*) as count FROM apartments
                WHERE first_seen > ? AND is_active = 1
            ''', (week_ago,))
            insights['new_this_week'] = cursor.fetchone()['count']

            # Price changes this week
            cursor.execute('''
                SELECT COUNT(DISTINCT apartment_id) as count
                FROM price_history
                WHERE recorded_at > ?
            ''', (week_ago,))
            insights['price_changes_this_week'] = cursor.fetchone()['count']

            # Most active neighborhoods
            cursor.execute('''
                SELECT neighborhood, COUNT(*) as count
                FROM apartments
                WHERE is_active = 1 AND neighborhood IS NOT NULL
                GROUP BY neighborhood
                ORDER BY count DESC
                LIMIT 10
            ''')
            insights['top_neighborhoods'] = [
                {'name': row['neighborhood'], 'count': row['count']}
                for row in cursor.fetchall()
            ]

            # Price distribution
            cursor.execute('''
                SELECT
                    CASE
                        WHEN price < 3000 THEN 'Under 3K'
                        WHEN price < 5000 THEN '3K-5K'
                        WHEN price < 7000 THEN '5K-7K'
                        WHEN price < 10000 THEN '7K-10K'
                        WHEN price < 15000 THEN '10K-15K'
                        ELSE '15K+'
                    END as price_range,
                    COUNT(*) as count
                FROM apartments
                WHERE is_active = 1
                GROUP BY price_range
                ORDER BY MIN(price)
            ''')
            insights['price_distribution'] = [
                {'range': row['price_range'], 'count': row['count']}
                for row in cursor.fetchall()
            ]

            insights['generated_at'] = datetime.now().isoformat()
            return insights

    def get_time_on_market(self, apt_id: str = None) -> Dict:
        """
        Calculate time on market for apartments.
        If apt_id provided, return for specific apartment.
        Otherwise, return statistics.
        """
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            if apt_id:
                cursor.execute('''
                    SELECT first_seen, last_seen, is_active
                    FROM apartments WHERE id = ?
                ''', (apt_id,))
                row = cursor.fetchone()

                if not row:
                    return {'error': 'Apartment not found'}

                first_seen = datetime.fromisoformat(row['first_seen'])
                last_seen = datetime.fromisoformat(row['last_seen'])

                if row['is_active']:
                    days = (datetime.now() - first_seen).days
                    status = 'active'
                else:
                    days = (last_seen - first_seen).days
                    status = 'removed'

                return {
                    'apartment_id': apt_id,
                    'first_seen': row['first_seen'],
                    'last_seen': row['last_seen'],
                    'days_on_market': days,
                    'status': status
                }

            # Statistics for all apartments
            cursor.execute('''
                SELECT
                    id,
                    first_seen,
                    last_seen,
                    is_active,
                    julianday(last_seen) - julianday(first_seen) as days_listed
                FROM apartments
            ''')
            rows = cursor.fetchall()

            if not rows:
                return {'message': 'No data available'}

            active_days = []
            removed_days = []

            for row in rows:
                if row['is_active']:
                    first = datetime.fromisoformat(row['first_seen'])
                    days = (datetime.now() - first).days
                    active_days.append(days)
                else:
                    removed_days.append(row['days_listed'] or 0)

            result = {
                'active_listings': {
                    'count': len(active_days),
                    'avg_days': round(statistics.mean(active_days)) if active_days else 0,
                    'median_days': round(statistics.median(active_days)) if active_days else 0,
                    'max_days': max(active_days) if active_days else 0
                },
                'removed_listings': {
                    'count': len(removed_days),
                    'avg_days': round(statistics.mean(removed_days)) if removed_days else 0,
                    'median_days': round(statistics.median(removed_days)) if removed_days else 0
                },
                'generated_at': datetime.now().isoformat()
            }

            return result

    def get_price_drop_alerts(self, min_drop_pct: float = 5.0) -> List[Dict]:
        """Find apartments with significant price drops"""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            # Get apartments with multiple price records
            cursor.execute('''
                SELECT
                    a.id, a.title, a.link, a.neighborhood, a.city,
                    ph1.price as old_price,
                    ph2.price as new_price,
                    ph1.recorded_at as old_date,
                    ph2.recorded_at as new_date
                FROM apartments a
                JOIN price_history ph1 ON a.id = ph1.apartment_id
                JOIN price_history ph2 ON a.id = ph2.apartment_id
                WHERE a.is_active = 1
                AND ph2.id = (
                    SELECT MAX(id) FROM price_history WHERE apartment_id = a.id
                )
                AND ph1.id = (
                    SELECT MAX(id) FROM price_history
                    WHERE apartment_id = a.id AND id < ph2.id
                )
                AND ph1.price > ph2.price
            ''')

            drops = []
            for row in cursor.fetchall():
                drop = row['old_price'] - row['new_price']
                drop_pct = (drop / row['old_price']) * 100

                if drop_pct >= min_drop_pct:
                    drops.append({
                        'id': row['id'],
                        'title': row['title'],
                        'link': row['link'],
                        'neighborhood': row['neighborhood'],
                        'old_price': row['old_price'],
                        'new_price': row['new_price'],
                        'drop': drop,
                        'drop_pct': round(drop_pct, 1),
                        'old_date': row['old_date'],
                        'new_date': row['new_date']
                    })

            return sorted(drops, key=lambda x: x['drop_pct'], reverse=True)

    def get_comparison(self, apt_id: str) -> Dict:
        """Compare apartment to market averages"""
        apt = self.db.get_apartment(apt_id)
        if not apt:
            return {'error': 'Apartment not found'}

        insights = self.get_market_insights()

        comparison = {
            'apartment': {
                'id': apt_id,
                'price': apt['price'],
                'rooms': apt.get('rooms'),
                'sqm': apt.get('sqm'),
                'neighborhood': apt.get('neighborhood')
            },
            'vs_market': {}
        }

        if apt['price'] and insights['overall']['avg_price']:
            diff = apt['price'] - insights['overall']['avg_price']
            diff_pct = (diff / insights['overall']['avg_price']) * 100
            comparison['vs_market']['price'] = {
                'market_avg': insights['overall']['avg_price'],
                'difference': round(diff),
                'difference_pct': round(diff_pct, 1),
                'status': 'above' if diff > 0 else 'below' if diff < 0 else 'at'
            }

        if apt.get('sqm') and apt['price']:
            apt_price_per_sqm = apt['price'] / apt['sqm']
            if insights.get('price_per_sqm', {}).get('avg'):
                market_avg = insights['price_per_sqm']['avg']
                diff = apt_price_per_sqm - market_avg
                diff_pct = (diff / market_avg) * 100
                comparison['vs_market']['price_per_sqm'] = {
                    'apartment': round(apt_price_per_sqm),
                    'market_avg': market_avg,
                    'difference': round(diff),
                    'difference_pct': round(diff_pct, 1),
                    'status': 'above' if diff > 0 else 'below' if diff < 0 else 'at'
                }

        return comparison

    def generate_weekly_report(self) -> str:
        """Generate a weekly market report in Hebrew"""
        insights = self.get_market_insights()
        trends = self.get_price_trends(days=7, group_by='all')
        time_stats = self.get_time_on_market()

        report = "📊 <b>דו\"ח שבועי - שוק הדירות</b>\n"
        report += "─" * 30 + "\n\n"

        # Overall stats
        report += f"🏠 <b>סיכום כללי:</b>\n"
        report += f"  • דירות פעילות: {insights['overall']['total_listings']}\n"
        report += f"  • מחיר ממוצע: ₪{insights['overall']['avg_price']:,}\n"
        report += f"  • טווח מחירים: ₪{insights['overall']['min_price']:,} - ₪{insights['overall']['max_price']:,}\n\n"

        # New listings
        report += f"🆕 <b>פעילות השבוע:</b>\n"
        report += f"  • דירות חדשות: {insights['new_this_week']}\n"
        report += f"  • שינויי מחיר: {insights['price_changes_this_week']}\n\n"

        # Price trend
        if trends.get('trends', {}).get('all'):
            trend = trends['trends']['all']
            emoji = "📈" if trend['direction'] == 'up' else "📉" if trend['direction'] == 'down' else "➡️"
            report += f"{emoji} <b>מגמת מחירים:</b>\n"
            report += f"  • שינוי שבועי: {trend['change_pct']:+.1f}%\n"
            report += f"  • מ-₪{trend['first_avg_price']:,} ל-₪{trend['last_avg_price']:,}\n\n"

        # Time on market
        if time_stats.get('active_listings'):
            report += f"⏱️ <b>זמן בשוק:</b>\n"
            report += f"  • ממוצע: {time_stats['active_listings']['avg_days']} ימים\n"
            report += f"  • חציון: {time_stats['active_listings']['median_days']} ימים\n\n"

        # Top neighborhoods
        if insights.get('top_neighborhoods'):
            report += f"📍 <b>שכונות מובילות:</b>\n"
            for n in insights['top_neighborhoods'][:5]:
                report += f"  • {n['name']}: {n['count']} דירות\n"

        report += f"\n<i>נוצר: {datetime.now().strftime('%d/%m/%Y %H:%M')}</i>"

        return report

    def generate_daily_digest(self, new_apartments: List[Dict], price_changes: List[Dict], removed_count: int) -> str:
        """Generate daily digest message"""
        report = "📬 <b>סיכום יומי</b>\n"
        report += "─" * 30 + "\n\n"

        # New apartments
        if new_apartments:
            report += f"🆕 <b>{len(new_apartments)} דירות חדשות</b>\n"
            for apt in new_apartments[:5]:
                price_str = f"₪{apt['price']:,}" if apt.get('price') else "לא צוין"
                report += f"  • {apt.get('title', 'ללא כותרת')[:30]} - {price_str}\n"
            if len(new_apartments) > 5:
                report += f"  ... ועוד {len(new_apartments) - 5} דירות\n"
            report += "\n"

        # Price changes
        if price_changes:
            drops = [p for p in price_changes if p.get('change', 0) < 0]
            increases = [p for p in price_changes if p.get('change', 0) > 0]

            if drops:
                report += f"📉 <b>{len(drops)} ירידות מחיר</b>\n"
                for p in drops[:3]:
                    report += f"  • {p['apartment'].get('title', '')[:25]}: ₪{p['old_price']:,} → ₪{p['new_price']:,}\n"
                report += "\n"

            if increases:
                report += f"📈 <b>{len(increases)} עליות מחיר</b>\n"

        # Removed
        if removed_count > 0:
            report += f"🗑️ <b>{removed_count} דירות הוסרו</b>\n\n"

        if not new_apartments and not price_changes and removed_count == 0:
            report += "😴 אין שינויים היום\n"

        report += f"\n<i>{datetime.now().strftime('%d/%m/%Y %H:%M')}</i>"

        return report
