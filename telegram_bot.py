"""
Telegram Bot Module for Yad2 Monitor
Handles webhook, commands, and inline keyboard interactions
"""
import os
import json
import logging
import requests
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class TelegramBot:
    """Telegram bot for multi-user apartment monitoring"""

    def __init__(self, token: str, database):
        self.token = token
        self.db = database
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.scrape_callback = None  # Set by monitor to allow /scrape command

    def set_webhook(self, webhook_url: str) -> bool:
        """Set the webhook URL for receiving updates"""
        try:
            url = f"{self.base_url}/setWebhook"
            data = {
                'url': webhook_url,
                'allowed_updates': ['message', 'callback_query']
            }
            response = requests.post(url, json=data, timeout=10)
            result = response.json()

            if result.get('ok'):
                logger.info(f"Webhook set successfully: {webhook_url}")
                return True
            else:
                logger.error(f"Failed to set webhook: {result}")
                return False
        except Exception as e:
            logger.error(f"Error setting webhook: {e}", exc_info=True)
            return False

    def send_message(self, chat_id: str, text: str, parse_mode: str = 'HTML',
                    reply_markup: Optional[Dict] = None) -> bool:
        """Send a text message to a chat"""
        try:
            url = f"{self.base_url}/sendMessage"
            data = {
                'chat_id': chat_id,
                'text': text,
                'parse_mode': parse_mode
            }
            if reply_markup:
                data['reply_markup'] = json.dumps(reply_markup)

            response = requests.post(url, json=data, timeout=10)
            result = response.json()

            if not result.get('ok'):
                logger.error(f"Failed to send message: {result}")
                return False
            return True
        except Exception as e:
            logger.error(f"Error sending message: {e}", exc_info=True)
            return False

    def answer_callback_query(self, callback_query_id: str, text: str = None,
                              show_alert: bool = False) -> bool:
        """Answer a callback query from inline keyboard"""
        try:
            url = f"{self.base_url}/answerCallbackQuery"
            data = {'callback_query_id': callback_query_id}
            if text:
                data['text'] = text
            data['show_alert'] = show_alert

            response = requests.post(url, json=data, timeout=10)
            return response.json().get('ok', False)
        except Exception as e:
            logger.error(f"Error answering callback: {e}", exc_info=True)
            return False

    def handle_webhook(self, update: Dict) -> Dict:
        """
        Handle incoming webhook update from Telegram.
        Returns response for Flask endpoint.
        """
        try:
            # Handle callback query (inline button clicks)
            if 'callback_query' in update:
                return self.handle_callback_query(update['callback_query'])

            # Handle regular messages
            if 'message' in update:
                message = update['message']
                chat_id = str(message['chat']['id'])

                # Register/update user
                self._register_user(message)

                # Handle commands
                if 'text' in message and message['text'].startswith('/'):
                    return self.handle_command(message)

                # Handle regular text messages
                return self.handle_text_message(message)

            return {'status': 'ok', 'message': 'Update received'}

        except Exception as e:
            logger.error(f"Error handling webhook: {e}", exc_info=True)
            return {'status': 'error', 'message': str(e)}

    def _register_user(self, message: Dict):
        """Register or update user from message"""
        chat = message['chat']
        user = message.get('from', chat)

        self.db.add_or_update_user(
            chat_id=str(chat['id']),
            username=user.get('username'),
            first_name=user.get('first_name'),
            last_name=user.get('last_name'),
            language_code=user.get('language_code', 'he')
        )

    def handle_command(self, message: Dict) -> Dict:
        """Route command to appropriate handler"""
        chat_id = str(message['chat']['id'])
        text = message['text']
        command_parts = text.split()
        command = command_parts[0].lower()
        args = command_parts[1:] if len(command_parts) > 1 else []

        # Command routing
        command_handlers = {
            '/start': self.cmd_start,
            '/help': self.cmd_help,
            '/status': self.cmd_status,
            '/stats': self.cmd_stats,
            '/favorites': self.cmd_favorites,
            '/search': self.cmd_search,
            '/filter': self.cmd_filter,
            '/pause': self.cmd_pause,
            '/resume': self.cmd_resume,
            '/analytics': self.cmd_analytics,
            '/scrape': self.cmd_scrape,
        }

        handler = command_handlers.get(command)
        if handler:
            handler(chat_id, args)
        else:
            self.send_message(chat_id,
                f"פקודה לא מוכרת: {command}\nשלח /help לרשימת הפקודות")

        return {'status': 'ok', 'command': command}

    def cmd_start(self, chat_id: str, args: List[str]):
        """Handle /start command"""
        user = self.db.get_user(chat_id)
        name = user.get('first_name', 'משתמש') if user else 'משתמש'

        text = f"""
🏠 <b>ברוך הבא ל-Yad2 Monitor, {name}!</b>

אני אעזור לך למצוא את הדירה המושלמת ביד2.
אקבל עדכונים על דירות חדשות, שינויי מחיר ועוד.

<b>📋 פקודות זמינות:</b>
/status - סטטוס המערכת והמשתמש
/stats - סטטיסטיקות שוק
/favorites - הצג מועדפים
/search [טקסט] - חיפוש דירות
/filter - ניהול פילטרים
/pause - השהה התראות
/resume - חידוש התראות
/scrape - סריקה מיידית של יד2
/analytics - תובנות שוק
/help - עזרה

💡 <b>טיפ:</b> השתמש בפקודת /filter כדי להגדיר את ההעדפות שלך ולקבל התראות רלוונטיות בלבד!
"""
        self.send_message(chat_id, text)

    def cmd_help(self, chat_id: str, args: List[str]):
        """Handle /help command"""
        text = """
📖 <b>מדריך שימוש - Yad2 Monitor</b>

<b>פקודות בסיסיות:</b>
/start - התחלה והרשמה
/help - מדריך זה
/status - סטטוס המערכת שלך

<b>חיפוש ומעקב:</b>
/search תל אביב 3 חדרים - חיפוש דירות
/favorites - רשימת המועדפים שלך
/filter - הגדרת פילטרים אישיים

<b>סטטיסטיקות:</b>
/stats - סטטיסטיקות שוק כלליות
/analytics - ניתוח מעמיק של השוק

<b>ניהול התראות:</b>
/pause - השהה התראות זמנית
/resume - חידוש התראות

<b>סריקה:</b>
/scrape - הפעל סריקה מיידית של יד2

<b>💡 דוגמאות שימוש:</b>
• <code>/search רמת גן</code> - חיפוש בעיר
• <code>/search 5000 7000</code> - חיפוש לפי טווח מחירים
• לחיצה על ⭐ בהתראה - הוספה למועדפים

<b>🔔 התראות אוטומטיות:</b>
תקבל התראות על:
• דירות חדשות שמתאימות לפילטרים שלך
• ירידות מחיר משמעותיות
• דירות שחזרו לשוק

זקוק לעזרה? צור קשר עם המפתח.
"""
        self.send_message(chat_id, text)

    def cmd_status(self, chat_id: str, args: List[str]):
        """Handle /status command"""
        user = self.db.get_user(chat_id)
        prefs = self.db.get_user_preferences(chat_id)
        favorites_count = len(self.db.get_user_favorites(chat_id))
        filters = self.db.get_user_filters(chat_id, active_only=True)

        status_emoji = "✅" if not user.get('is_paused') else "⏸️"
        status_text = "פעיל" if not user.get('is_paused') else "מושהה"

        text = f"""
👤 <b>סטטוס המשתמש</b>

{status_emoji} <b>סטטוס:</b> {status_text}
⭐ <b>מועדפים:</b> {favorites_count} דירות
🔍 <b>פילטרים פעילים:</b> {len(filters)}

<b>🔔 הגדרות התראות:</b>
• התראות מיידיות: {'✓' if prefs.get('instant_notifications') else '✗'}
• דייג'סט יומי: {'✓' if prefs.get('daily_digest') else '✗'}
• שעת דייג'סט: {prefs.get('digest_hour', 20)}:00

<b>📊 מידע כללי:</b>
• תאריך הצטרפות: {user.get('created_at', 'לא ידוע')[:10] if user else 'לא ידוע'}
• אינטראקציה אחרונה: {user.get('last_interaction', 'עכשיו')[:16] if user else 'עכשיו'}

להשהיית התראות: /pause
לחידוש התראות: /resume
"""
        self.send_message(chat_id, text)

    def cmd_stats(self, chat_id: str, args: List[str]):
        """Handle /stats command"""
        try:
            # Get overall stats
            all_apts = self.db.get_active_apartments()
            total = len(all_apts)

            if total == 0:
                self.send_message(chat_id, "אין דירות במעקב כרגע 🤷‍♂️")
                return

            prices = [apt['price'] for apt in all_apts if apt.get('price')]
            avg_price = sum(prices) // len(prices) if prices else 0

            # Get recent stats
            recent_apts = [apt for apt in all_apts if apt.get('first_seen', '').startswith(datetime.now().strftime('%Y-%m-%d'))]
            new_today = len(recent_apts)

            text = f"""
📊 <b>סטטיסטיקות שוק - Yad2</b>

<b>🏢 דירות במעקב:</b>
• סה"כ דירות פעילות: {total:,}
• חדשות היום: {new_today}

<b>💰 מחירים:</b>
• מחיר ממוצע: ₪{avg_price:,}
• מחיר נמוך ביותר: ₪{min(prices):,}
• מחיר גבוה ביותר: ₪{max(prices):,}

<b>🔥 ערים פופולריות:</b>
"""
            # Top cities
            cities = {}
            for apt in all_apts:
                city = apt.get('city', 'לא ידוע')
                cities[city] = cities.get(city, 0) + 1

            top_cities = sorted(cities.items(), key=lambda x: x[1], reverse=True)[:5]
            for city, count in top_cities:
                text += f"• {city}: {count} דירות\n"

            text += "\nלניתוח מפורט: /analytics"

            self.send_message(chat_id, text)
        except Exception as e:
            logger.error(f"Error in cmd_stats: {e}", exc_info=True)
            self.send_message(chat_id, "שגיאה בטעינת סטטיסטיקות 😞")

    def cmd_favorites(self, chat_id: str, args: List[str]):
        """Handle /favorites command"""
        favorites = self.db.get_user_favorites(chat_id)

        if not favorites:
            self.send_message(chat_id, "אין לך מועדפים עדיין 🤷‍♂️\n\nלחץ על ⭐ בהתראות כדי להוסיף דירות למועדפים!")
            return

        text = f"⭐ <b>הדירות המועדפות שלך</b> ({len(favorites)})\n\n"

        for i, apt in enumerate(favorites[:10], 1):
            price = f"₪{apt['price']:,}" if apt.get('price') else 'לא ידוע'
            rooms = f"{apt['rooms']} חד'" if apt.get('rooms') else 'לא ידוע'
            location = apt.get('city', apt.get('location', 'לא ידוע'))

            text += f"{i}. <b>{apt.get('title', 'ללא כותרת')[:40]}</b>\n"
            text += f"   📍 {location} | 🛏️ {rooms} | 💰 {price}\n"
            text += f"   <a href=\"{apt['link']}\">🔗 צפייה ביד2</a>\n\n"

        if len(favorites) > 10:
            text += f"\n... ועוד {len(favorites) - 10} דירות"

        self.send_message(chat_id, text)

    def cmd_search(self, chat_id: str, args: List[str]):
        """Handle /search command"""
        if not args:
            self.send_message(chat_id,
                "🔍 <b>חיפוש דירות</b>\n\n"
                "דוגמאות שימוש:\n"
                "• <code>/search תל אביב</code>\n"
                "• <code>/search רמת גן 3 חדרים</code>\n"
                "• <code>/search 5000 8000</code> (טווח מחירים)")
            return

        query = ' '.join(args).lower()
        all_apts = self.db.get_active_apartments()

        # Filter apartments based on query
        results = []
        for apt in all_apts:
            searchable = ' '.join([
                str(apt.get('title', '')),
                str(apt.get('city', '')),
                str(apt.get('neighborhood', '')),
                str(apt.get('location', ''))
            ]).lower()

            if query in searchable:
                results.append(apt)

        if not results:
            self.send_message(chat_id, f"לא נמצאו תוצאות עבור: {query} 😞")
            return

        text = f"🔍 <b>תוצאות חיפוש</b> ({len(results)})\n\n"

        for i, apt in enumerate(results[:8], 1):
            price = f"₪{apt['price']:,}" if apt.get('price') else 'לא ידוע'
            rooms = f"{apt['rooms']} חד'" if apt.get('rooms') else ''
            location = apt.get('city', apt.get('location', ''))

            text += f"{i}. {apt.get('title', 'ללא כותרת')[:50]}\n"
            text += f"   📍 {location} | 💰 {price}"
            if rooms:
                text += f" | 🛏️ {rooms}"
            text += f"\n   <a href=\"{apt['link']}\">🔗 צפייה</a>\n\n"

        if len(results) > 8:
            text += f"\n... ועוד {len(results) - 8} תוצאות"

        self.send_message(chat_id, text)

    def cmd_filter(self, chat_id: str, args: List[str]):
        """Handle /filter command"""
        filters = self.db.get_user_filters(chat_id, active_only=False)

        text = "🔍 <b>ניהול פילטרים</b>\n\n"

        if filters:
            text += "<b>הפילטרים שלך:</b>\n"
            for f in filters:
                status = "✓" if f['is_active'] else "✗"
                name = f['name']
                ftype = f['filter_type']

                if ftype in ['price', 'rooms', 'sqm']:
                    min_val = f['min_value'] if f['min_value'] else ''
                    max_val = f['max_value'] if f['max_value'] else ''
                    text += f"{status} {name}: {min_val}-{max_val}\n"
                else:
                    text += f"{status} {name}: {f['text_value']}\n"
        else:
            text += "אין לך פילטרים מוגדרים.\n\n"

        text += """
<b>💡 כיצד להגדיר פילטרים:</b>
השתמש בדאשבורד האינטרנטי שלנו להגדרת פילטרים מתקדמים!

הפילטרים מאפשרים לך לקבל התראות רק על דירות שמתאימות להעדפות שלך.
"""
        self.send_message(chat_id, text)

    def cmd_pause(self, chat_id: str, args: List[str]):
        """Handle /pause command"""
        self.db.pause_user_notifications(chat_id, paused=True)
        self.send_message(chat_id,
            "⏸️ <b>התראות הושהו</b>\n\n"
            "לא תקבל התראות חדשות עד שתחדש.\n"
            "לחידוש: /resume")

    def cmd_resume(self, chat_id: str, args: List[str]):
        """Handle /resume command"""
        self.db.pause_user_notifications(chat_id, paused=False)
        self.send_message(chat_id,
            "▶️ <b>התראות חודשו!</b>\n\n"
            "תתחיל לקבל התראות שוב על דירות חדשות ושינויי מחיר.")

    def cmd_scrape(self, chat_id: str, args: List[str]):
        """Handle /scrape command - trigger immediate scrape"""
        if not self.scrape_callback:
            self.send_message(chat_id, "⚠️ פונקציית הסריקה לא זמינה כרגע.")
            return

        self.send_message(chat_id, "🔍 <b>מתחיל סריקה מיידית...</b>\nזה עשוי לקחת כמה דקות.")

        try:
            import threading
            def run_scrape():
                try:
                    new_count, change_count = self.scrape_callback()
                    self.send_message(chat_id,
                        f"✅ <b>סריקה הושלמה!</b>\n\n"
                        f"🆕 דירות חדשות: {new_count}\n"
                        f"💰 שינויי מחיר: {change_count}")
                except Exception as e:
                    self.send_message(chat_id, f"❌ שגיאה בסריקה: {e}")

            threading.Thread(target=run_scrape, daemon=True).start()
        except Exception as e:
            self.send_message(chat_id, f"❌ שגיאה: {e}")

    def cmd_analytics(self, chat_id: str, args: List[str]):
        """Handle /analytics command"""
        text = """
📊 <b>אנליטיקת שוק מתקדמת</b>

לאנליזה מפורטת עם גרפים וויזואליזציות,
בקר בדאשבורד האינטרנטי שלנו:

🌐 <a href="http://localhost:5000">פתח דאשבורד</a>

בדאשבורד תמצא:
• 📈 גרפים של התפלגות מחירים
• 🗺️ ניתוח לפי שכונות
• 📉 מגמות מחירים לאורך זמן
• 🔥 תובנות שוק מתקדמות
"""
        self.send_message(chat_id, text)

    def handle_text_message(self, message: Dict) -> Dict:
        """Handle regular text messages"""
        chat_id = str(message['chat']['id'])
        self.send_message(chat_id,
            "אני מבין רק פקודות 🤖\n"
            "שלח /help לרשימת הפקודות הזמינות.")
        return {'status': 'ok'}

    def handle_callback_query(self, callback_query: Dict) -> Dict:
        """Handle inline keyboard button clicks"""
        try:
            query_id = callback_query['id']
            chat_id = str(callback_query['from']['id'])
            data = callback_query['data']

            # Parse callback data
            parts = data.split(':')
            action = parts[0]
            apt_id = parts[1] if len(parts) > 1 else None

            if action == 'favorite' and apt_id:
                self.db.add_user_favorite(chat_id, apt_id)
                self.answer_callback_query(query_id, "✓ נוסף למועדפים!")

            elif action == 'ignore' and apt_id:
                self.db.add_user_ignored(chat_id, apt_id)
                self.answer_callback_query(query_id, "✓ הדירה תתעלם")

            elif action == 'open' and apt_id:
                self.answer_callback_query(query_id, "פותח ביד2...")

            else:
                self.answer_callback_query(query_id, "פעולה לא מוכרת")

            return {'status': 'ok', 'action': action}

        except Exception as e:
            logger.error(f"Error handling callback: {e}", exc_info=True)
            return {'status': 'error', 'message': str(e)}

    def create_inline_keyboard(self, apt_id: str) -> Dict:
        """Create inline keyboard with buttons for apartment notification"""
        return {
            'inline_keyboard': [[
                {'text': '⭐ הוסף למועדפים', 'callback_data': f'favorite:{apt_id}'},
                {'text': '🚫 התעלם', 'callback_data': f'ignore:{apt_id}'}
            ], [
                {'text': '🔗 פתח ביד2', 'url': f'https://www.yad2.co.il/item/{apt_id}'}
            ]]
        }

    def format_apartment_notification(self, apartment: Dict, notification_type: str = 'new') -> str:
        """Format apartment data into a nice notification message"""
        title = apartment.get('title', 'ללא כותרת')
        price = f"₪{apartment['price']:,}" if apartment.get('price') else 'לא ידוע'
        rooms = apartment.get('rooms', 'לא ידוע')
        sqm = apartment.get('sqm', 'לא ידוע')
        floor = apartment.get('floor', 'לא ידוע')
        city = apartment.get('city', '')
        neighborhood = apartment.get('neighborhood', '')
        location = f"{city}, {neighborhood}" if city and neighborhood else city or neighborhood or 'לא ידוע'

        if notification_type == 'new':
            emoji = "🆕"
            header = "<b>דירה חדשה נמצאה!</b>"
        elif notification_type == 'price_drop':
            emoji = "📉"
            header = "<b>ירידת מחיר!</b>"
            old_price = apartment.get('old_price')
            if old_price:
                drop_pct = ((old_price - apartment['price']) / old_price) * 100
                header += f" ({drop_pct:.1f}%-)"
        else:
            emoji = "🏠"
            header = "<b>עדכון דירה</b>"

        text = f"""
{emoji} {header}

<b>{title}</b>

📍 <b>מיקום:</b> {location}
💰 <b>מחיר:</b> {price}
🛏️ <b>חדרים:</b> {rooms}
📏 <b>מ"ר:</b> {sqm}
🏢 <b>קומה:</b> {floor}
"""

        if notification_type == 'price_drop' and apartment.get('old_price'):
            text += f"\n💸 <b>מחיר קודם:</b> ₪{apartment['old_price']:,}"

        return text.strip()

    def notify_new_apartment(self, apartment: Dict, target_users: List[str] = None):
        """Send new apartment notification to users"""
        apt_id = apartment['id']
        message_text = self.format_apartment_notification(apartment, 'new')
        keyboard = self.create_inline_keyboard(apt_id)

        # Get target users
        if target_users is None:
            target_users = [u['chat_id'] for u in self.db.get_all_active_users()]

        for chat_id in target_users:
            # Check if apartment matches user's filters
            if not self.db.apartment_matches_user_filters(chat_id, apartment):
                continue

            # Check if user already ignored this apartment
            if self.db.is_user_ignored(chat_id, apt_id):
                continue

            self.send_message(chat_id, message_text, reply_markup=keyboard)

    def notify_price_change(self, apartment: Dict, old_price: float, target_users: List[str] = None):
        """Send price change notification to users"""
        apartment['old_price'] = old_price
        apt_id = apartment['id']
        message_text = self.format_apartment_notification(apartment, 'price_drop')
        keyboard = self.create_inline_keyboard(apt_id)

        # Get target users
        if target_users is None:
            target_users = [u['chat_id'] for u in self.db.get_all_active_users()]

        for chat_id in target_users:
            # Check if apartment matches user's filters
            if not self.db.apartment_matches_user_filters(chat_id, apartment):
                continue

            # Check if user already ignored this apartment
            if self.db.is_user_ignored(chat_id, apt_id):
                continue

            self.send_message(chat_id, message_text, reply_markup=keyboard)
