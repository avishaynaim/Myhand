"""
Notifications Module for Yad2 Monitor
Handles Telegram notifications with filters, summaries, and rich messages
"""
import requests
import time
import os
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor
import logging

logger = logging.getLogger(__name__)


class NotificationManager:
    """Manages notifications across multiple channels"""

    def __init__(self, database, telegram_bot=None):
        self.db = database
        self.telegram_bot = telegram_bot

        # Fallback for legacy single-user support
        self.telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID')

        # Server identification
        self.server_name = os.environ.get('SERVER_NAME') or \
                          os.environ.get('RAILWAY_SERVICE_NAME') or \
                          os.environ.get('RAILWAY_PROJECT_NAME') or \
                          'Yad2-Monitor'

        self.notification_queue: List[Dict] = []
        self.daily_notifications: List[Dict] = []  # Collected for daily digest

        # Thread safety locks
        self._queue_lock = threading.Lock()
        self._daily_lock = threading.Lock()

        # Settings
        self.instant_notifications = True
        self.daily_digest_enabled = True
        self.daily_digest_hour = 20  # 8 PM
        self.last_digest_sent = None

        # Rate limiting
        self.last_message_time = 0
        self.min_message_interval = 0.5  # seconds between messages

    def get_server_signature(self) -> str:
        """Get server signature for messages"""
        return f"\n\n🖥️ <i>{self.server_name}</i>"

    def should_notify(self, apt: Dict, notification_type: str = 'new') -> bool:
        """Check if we should send notification for this apartment based on filters"""
        # Check if apartment is ignored
        ignored = self.db.get_ignored_ids()
        if apt.get('id') in ignored:
            return False

        # Check filters
        return self.db.apartment_passes_filters(apt)

    def format_new_apartment_message(self, apt: Dict, rich: bool = True) -> str:
        """Format message for new apartment"""
        if not rich:
            return f"New apartment: {apt.get('title')} - ₪{apt.get('price', 0):,}"

        price = apt.get('price', 0)
        price_str = f"₪{price:,}" if price else "לא צוין"

        # Calculate price per sqm if available
        sqm = apt.get('sqm')
        price_per_sqm = ""
        if sqm and sqm > 0 and price:
            pps = price / sqm
            price_per_sqm = f"\n💵 <b>מחיר למ\"ר:</b> ₪{pps:,.0f}"

        # Format rooms
        rooms = apt.get('rooms')
        rooms_str = f"{rooms}" if rooms else "לא צוין"

        # Floor info
        floor = apt.get('floor')
        floor_str = f"\n🏢 <b>קומה:</b> {floor}" if floor else ""

        # Item info (usually contains rooms, sqm, floor)
        item_info = apt.get('item_info', '')
        info_line = f"\n📋 {item_info}" if item_info else ""

        timestamp = datetime.now().strftime('%d/%m/%Y %H:%M')

        message = (
            f"🆕 <b>דירה חדשה!</b>\n"
            f"{'─' * 30}\n\n"
            f"<b>📍 {apt.get('title', 'ללא כותרת')}</b>\n\n"
            f"🏠 <b>כתובת:</b> {apt.get('street_address') or apt.get('location') or 'לא צוין'}\n"
            f"{info_line}"
            f"\n💰 <b>מחיר:</b> {price_str}"
            f"{price_per_sqm}"
            f"{floor_str}\n"
            f"📅 <b>תאריך:</b> {timestamp}\n\n"
            f"🔗 <a href='{apt.get('link', '')}'>לצפייה בדירה</a>"
            f"{self.get_server_signature()}"
        )

        return message

    def format_price_change_message(self, apt: Dict, old_price: int, new_price: int,
                                     rich: bool = True) -> str:
        """Format message for price change"""
        change = new_price - old_price
        change_pct = (change / old_price) * 100 if old_price > 0 else 0

        if not rich:
            direction = "↓" if change < 0 else "↑"
            return f"Price {direction}: {apt.get('title')} - ₪{old_price:,} → ₪{new_price:,}"

        emoji = "📉" if change < 0 else "📈"
        change_text = "ירידת מחיר!" if change < 0 else "עליית מחיר"
        change_emoji = "🔽" if change < 0 else "🔼"

        # Add recommendation for significant drops
        recommendation = ""
        if change < 0 and abs(change_pct) >= 5:
            recommendation = "\n\n⭐ <b>ירידה משמעותית - שווה לבדוק!</b>"

        message = (
            f"{emoji} <b>{change_text}</b>\n"
            f"{'─' * 30}\n\n"
            f"<b>📍 {apt.get('title', 'ללא כותרת')}</b>\n\n"
            f"💵 <b>מחיר קודם:</b> ₪{old_price:,}\n"
            f"💰 <b>מחיר חדש:</b> ₪{new_price:,}\n"
            f"{change_emoji} <b>שינוי:</b> ₪{abs(change):,} ({change_pct:+.1f}%)"
            f"{recommendation}\n\n"
            f"🔗 <a href='{apt.get('link', '')}'>לצפייה בדירה</a>"
            f"{self.get_server_signature()}"
        )

        return message

    def format_removed_message(self, apt: Dict) -> str:
        """Format message for removed apartment"""
        # Calculate time on market
        first_seen = apt.get('first_seen')
        days_on_market = 0
        if first_seen:
            try:
                first_dt = datetime.fromisoformat(first_seen)
                days_on_market = (datetime.now() - first_dt).days
            except:
                pass

        message = (
            f"🗑️ <b>דירה הוסרה</b>\n"
            f"{'─' * 30}\n\n"
            f"<b>📍 {apt.get('title', 'ללא כותרת')}</b>\n"
            f"💰 <b>מחיר אחרון:</b> ₪{apt.get('price', 0):,}\n"
            f"📅 <b>ימים בשוק:</b> {days_on_market}\n"
        )

        return message

    def format_daily_digest(self, new_apartments: List[Dict], price_changes: List[Dict],
                           removed: List[Dict]) -> str:
        """Format daily digest message"""
        message = "📬 <b>סיכום יומי - Yad2 Monitor</b>\n"
        message += "─" * 30 + "\n\n"

        # Summary counts
        message += f"📊 <b>סיכום:</b>\n"
        message += f"  🆕 דירות חדשות: {len(new_apartments)}\n"
        message += f"  💰 שינויי מחיר: {len(price_changes)}\n"
        message += f"  🗑️ הוסרו: {len(removed)}\n\n"

        # Top new apartments (by lowest price)
        if new_apartments:
            message += "🆕 <b>דירות חדשות (הזולות ביותר):</b>\n"
            sorted_new = sorted(new_apartments, key=lambda x: x.get('price', float('inf')))
            for apt in sorted_new[:5]:
                price = f"₪{apt.get('price', 0):,}" if apt.get('price') else "לא צוין"
                message += f"  • {apt.get('title', '')[:35]} - {price}\n"
            if len(new_apartments) > 5:
                message += f"  ... ועוד {len(new_apartments) - 5}\n"
            message += "\n"

        # Price drops
        drops = [p for p in price_changes if p.get('change', 0) < 0]
        if drops:
            message += "📉 <b>ירידות מחיר:</b>\n"
            sorted_drops = sorted(drops, key=lambda x: x.get('change_pct', 0))
            for p in sorted_drops[:5]:
                apt = p.get('apartment', {})
                message += f"  • {apt.get('title', '')[:30]}: {p.get('change_pct', 0):.1f}%\n"
            message += "\n"

        # Market stats if available
        total_active = self.db.get_setting('total_active_listings')
        if total_active:
            message += f"📈 <b>שוק:</b> {total_active} דירות פעילות\n"

        message += f"\n<i>נשלח: {datetime.now().strftime('%d/%m/%Y %H:%M')}</i>"

        return message

    def send_telegram_message(self, message: str, max_retries: int = 3,
                              disable_preview: bool = False) -> bool:
        """Send message via Telegram"""
        if not self.telegram_token or not self.telegram_chat_id:
            logger.warning("Telegram credentials not configured")
            return False

        # Rate limiting
        elapsed = time.time() - self.last_message_time
        if elapsed < self.min_message_interval:
            time.sleep(self.min_message_interval - elapsed)

        for attempt in range(max_retries):
            try:
                url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                data = {
                    'chat_id': self.telegram_chat_id,
                    'text': message,
                    'parse_mode': 'HTML',
                    'disable_web_page_preview': disable_preview
                }

                response = requests.post(url, data=data, timeout=10)
                self.last_message_time = time.time()

                if response.status_code == 200:
                    return True
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 30))
                    logger.warning(f"Rate limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                else:
                    logger.error(f"Telegram error: {response.status_code} - {response.text}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return False

            except Exception as e:
                logger.error(f"Failed to send Telegram message: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return False

        return False

    def send_telegram_photo(self, photo_url: str, caption: str) -> bool:
        """Send photo with caption via Telegram"""
        if not self.telegram_token or not self.telegram_chat_id:
            return False

        try:
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendPhoto"
            data = {
                'chat_id': self.telegram_chat_id,
                'photo': photo_url,
                'caption': caption,
                'parse_mode': 'HTML'
            }
            response = requests.post(url, data=data, timeout=15)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Failed to send photo: {e}")
            return False

    def send_telegram_with_buttons(self, message: str, buttons: List[List[Dict]]) -> bool:
        """Send message with inline keyboard buttons"""
        if not self.telegram_token or not self.telegram_chat_id:
            return False

        try:
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            data = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'HTML',
                'reply_markup': json.dumps({
                    'inline_keyboard': buttons
                })
            }
            response = requests.post(url, json=data, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Failed to send message with buttons: {e}")
            return False

    def notify_new_apartment(self, apt: Dict):
        """Send notification for new apartment to all active users"""
        if not self.should_notify(apt, 'new'):
            return

        # Multi-user notification via TelegramBot
        if self.telegram_bot and self.instant_notifications:
            try:
                self.telegram_bot.notify_new_apartment(apt)
            except Exception as e:
                logger.error(f"Error sending multi-user notification: {e}", exc_info=True)
                # Fallback to legacy single-user notification
                message = self.format_new_apartment_message(apt)
                self.send_telegram_message(message)
        elif self.instant_notifications:
            # Legacy single-user notification
            message = self.format_new_apartment_message(apt)
            self.send_telegram_message(message)

        # Store for daily digest (thread-safe)
        with self._daily_lock:
            self.daily_notifications.append({
                'type': 'new',
                'apartment': apt,
                'timestamp': datetime.now().isoformat()
            })

    def notify_price_change(self, apt: Dict, old_price: int, new_price: int):
        """Send notification for price change to all active users"""
        if not self.should_notify(apt, 'price_change'):
            return

        # Multi-user notification via TelegramBot
        if self.telegram_bot and self.instant_notifications:
            try:
                self.telegram_bot.notify_price_change(apt, old_price)
            except Exception as e:
                logger.error(f"Error sending multi-user notification: {e}", exc_info=True)
                # Fallback to legacy single-user notification
                message = self.format_price_change_message(apt, old_price, new_price)
                self.send_telegram_message(message)
        elif self.instant_notifications:
            # Legacy single-user notification
            message = self.format_price_change_message(apt, old_price, new_price)
            self.send_telegram_message(message)

        # Store for daily digest (thread-safe)
        with self._daily_lock:
            self.daily_notifications.append({
                'type': 'price_change',
                'apartment': apt,
                'old_price': old_price,
                'new_price': new_price,
                'change': new_price - old_price,
                'change_pct': ((new_price - old_price) / old_price) * 100 if old_price else 0,
                'timestamp': datetime.now().isoformat()
            })

    def notify_removed(self, apt: Dict):
        """Send notification for removed apartment (optional - can be noisy)"""
        # Usually we don't send instant notifications for removed apartments
        # Store for daily digest (thread-safe)
        with self._daily_lock:
            self.daily_notifications.append({
                'type': 'removed',
                'apartment': apt,
                'timestamp': datetime.now().isoformat()
            })

    def send_batch_notifications(self, new_apartments: List[Dict], price_changes: List[Dict]):
        """Send batch notifications efficiently"""
        messages = []

        for apt in new_apartments:
            if self.should_notify(apt, 'new'):
                messages.append(self.format_new_apartment_message(apt))

        for change in price_changes:
            apt = change.get('apartment', {})
            if self.should_notify(apt, 'price_change'):
                messages.append(self.format_price_change_message(
                    apt, change['old_price'], change['new_price']
                ))

        if not messages:
            return

        # Send in parallel with rate limiting
        with ThreadPoolExecutor(max_workers=3) as executor:
            for i, msg in enumerate(messages):
                # Stagger messages to avoid rate limiting
                if i > 0:
                    time.sleep(0.5)
                executor.submit(self.send_telegram_message, msg)

    def send_daily_digest(self):
        """Send daily digest if enabled and not already sent today"""
        if not self.daily_digest_enabled:
            return

        # Check if already sent today
        today = datetime.now().strftime('%Y-%m-%d')
        summary = self.db.get_daily_summary(today)
        if summary and summary.get('summary_sent'):
            return

        # Get today's data (thread-safe)
        with self._daily_lock:
            new_apartments = [n['apartment'] for n in self.daily_notifications if n['type'] == 'new']
            price_changes = [n for n in self.daily_notifications if n['type'] == 'price_change']
            removed = [n['apartment'] for n in self.daily_notifications if n['type'] == 'removed']

        if not new_apartments and not price_changes and not removed:
            return

        message = self.format_daily_digest(new_apartments, price_changes, removed)
        success = self.send_telegram_message(message, disable_preview=True)

        if success:
            self.db.mark_summary_sent(today)
            with self._daily_lock:
                self.daily_notifications.clear()

    def check_daily_digest_time(self):
        """Check if it's time to send daily digest"""
        now = datetime.now()
        if now.hour == self.daily_digest_hour:
            today = now.strftime('%Y-%m-%d')
            summary = self.db.get_daily_summary(today)
            if not summary or not summary.get('summary_sent'):
                self.send_daily_digest()

    def send_status_report(self, scrape_stats: Dict, delay_multiplier: float = 1.0):
        """Send scraper status report"""
        total = sum(scrape_stats.values())
        success = scrape_stats.get('success', 0)
        success_rate = (success / total * 100) if total > 0 else 0

        message = (
            f"📊 <b>סטטוס Monitor</b>\n"
            f"{'─' * 25}\n\n"
            f"📈 <b>24 שעות אחרונות:</b>\n"
            f"  ✅ הצלחות: {success}\n"
            f"  🚫 חסימות: {scrape_stats.get('block', 0)}\n"
            f"  ⏳ Rate limits: {scrape_stats.get('rate_limit', 0)}\n"
            f"  ❌ שגיאות: {scrape_stats.get('error', 0)}\n"
            f"  📊 אחוז הצלחה: {success_rate:.1f}%\n\n"
            f"⚙️ <b>הגדרות:</b>\n"
            f"  🔄 מכפיל השהיה: {delay_multiplier:.2f}x"
            f"{self.get_server_signature()}"
        )

        self.send_telegram_message(message)

    def send_error_alert(self, error: str, context: str = None):
        """Send error alert"""
        message = (
            f"❌ <b>שגיאה ב-Yad2 Monitor</b>\n"
            f"{'─' * 25}\n\n"
            f"<code>{error}</code>"
        )
        if context:
            message += f"\n\n📝 <b>הקשר:</b> {context}"

        message += f"\n\n⏰ {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
        message += self.get_server_signature()

        self.send_telegram_message(message)

    def send_startup_message(self, config: Dict = None):
        """Send startup notification"""
        message = (
            f"🤖 <b>Yad2 Monitor הופעל!</b>\n"
            f"{'─' * 25}\n\n"
            f"🔄 <b>מערכת אדפטיבית:</b> פעילה\n"
            f"🧠 <b>עצירה חכמה:</b> פעילה\n"
        )

        if config:
            message += f"⏱️ <b>מרווח:</b> {config.get('min_interval', 60)}-{config.get('max_interval', 90)} דקות\n"

        message += f"\n🔍 <b>סטטוס:</b> מנטר..."
        message += self.get_server_signature()

        self.send_telegram_message(message)


class TelegramBotHandler:
    """Handle Telegram bot commands (optional - for interactive features)"""

    def __init__(self, notification_manager: NotificationManager, database):
        self.notifier = notification_manager
        self.db = database
        self.commands = {
            '/status': self.cmd_status,
            '/stats': self.cmd_stats,
            '/favorites': self.cmd_favorites,
            '/help': self.cmd_help,
            '/pause': self.cmd_pause,
            '/resume': self.cmd_resume,
        }

    def handle_update(self, update: Dict):
        """Handle incoming Telegram update"""
        message = update.get('message', {})
        text = message.get('text', '')

        if text.startswith('/'):
            cmd = text.split()[0].lower()
            if cmd in self.commands:
                self.commands[cmd](message)

    def cmd_status(self, message: Dict):
        """Handle /status command"""
        stats = self.db.get_scrape_stats(hours=24)
        self.notifier.send_status_report(stats)

    def cmd_stats(self, message: Dict):
        """Handle /stats command - market statistics"""
        # Would need analytics module
        pass

    def cmd_favorites(self, message: Dict):
        """Handle /favorites command"""
        favorites = self.db.get_favorites()
        if not favorites:
            self.notifier.send_telegram_message("📋 אין מועדפים")
            return

        msg = "⭐ <b>מועדפים:</b>\n\n"
        for apt in favorites[:10]:
            price = f"₪{apt['price']:,}" if apt.get('price') else "לא צוין"
            msg += f"• {apt.get('title', '')[:30]} - {price}\n"

        self.notifier.send_telegram_message(msg)

    def cmd_help(self, message: Dict):
        """Handle /help command"""
        msg = (
            "📚 <b>פקודות זמינות:</b>\n\n"
            "/status - סטטוס המערכת\n"
            "/stats - סטטיסטיקות שוק\n"
            "/favorites - צפייה במועדפים\n"
            "/pause - השהיית התראות\n"
            "/resume - חידוש התראות\n"
        )
        self.notifier.send_telegram_message(msg)

    def cmd_pause(self, message: Dict):
        """Handle /pause command"""
        self.notifier.instant_notifications = False
        self.notifier.send_telegram_message("⏸️ התראות מיידיות הושהו")

    def cmd_resume(self, message: Dict):
        """Handle /resume command"""
        self.notifier.instant_notifications = True
        self.notifier.send_telegram_message("▶️ התראות מיידיות חודשו")
