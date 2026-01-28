"""
Yad2 Real Estate Monitor - Enhanced Version
Features:
- SQLite persistent storage
- Price history tracking
- Proxy rotation
- Market analytics
- Web dashboard with REST API
- Telegram notifications with filters
- Daily summaries
- Smart pagination
- Adaptive delay management
"""
import requests
from bs4 import BeautifulSoup
import time
import json
import os
import re
import random
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

# Import our modules
from db_wrapper import get_database
from proxy_manager import ProxyManager, ProxyRotator
from analytics import MarketAnalytics
from notifications import NotificationManager
from web import create_web_app, run_web_server

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('yad2_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_database_path():
    """
    Get the database path with persistent storage support.

    For Railway persistent storage:
    1. Add PostgreSQL database (free, persistent)
    2. Or add Volume mounted at /data
    """
    # Check for explicit environment variable
    db_path = os.environ.get('DATABASE_PATH')
    if db_path:
        return db_path

    # Check for persistent volume mount at /data
    data_dir = '/data'
    if os.path.exists(data_dir) and os.path.isdir(data_dir):
        db_file = os.path.join(data_dir, 'yad2_monitor.db')
        logger.info(f"✅ Using persistent storage: {db_file}")
        return db_file

    # Check if PostgreSQL is available (Railway provides DATABASE_URL)
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        logger.warning(f"⚠️  DATABASE_URL detected but SQLite is being used")
        logger.warning(f"⚠️  For PostgreSQL support, database.py needs to be updated")
        logger.warning(f"⚠️  Currently using ephemeral SQLite - data will be lost on deploy!")

    # Default to current directory (EPHEMERAL on Railway!)
    db_file = 'yad2_monitor.db'
    if 'RAILWAY_ENVIRONMENT' in os.environ:
        logger.error(f"❌ USING EPHEMERAL STORAGE: {db_file}")
        logger.error(f"❌ DATA WILL BE DELETED ON EVERY DEPLOYMENT!")
        logger.error(f"❌ Add PostgreSQL in Railway dashboard to fix this!")
        logger.error(f"❌ Go to: https://railway.app → New → Database → PostgreSQL")
    else:
        logger.info(f"📁 Using local storage: {db_file}")

    return db_file


class AdaptiveDelayManager:
    """Analyzes historical scraping data and adapts delays to avoid blocks."""

    def __init__(self, database):
        self.db = database
        self.base_page_delay = (5, 15)
        self.base_cycle_delay = (60, 90)
        self.current_multiplier = 1.0
        self._load_strategy()

    def _load_strategy(self):
        """Load strategy from database"""
        multiplier = self.db.get_setting('delay_multiplier')
        if multiplier:
            self.current_multiplier = float(multiplier)

    def _save_strategy(self):
        """Save strategy to database"""
        self.db.set_setting('delay_multiplier', str(self.current_multiplier))

    def get_last_run_timestamp(self) -> Optional[int]:
        """Get timestamp of last successful run in milliseconds."""
        ts = self.db.get_setting('last_run_timestamp')
        return int(ts) if ts else None

    def set_last_run_timestamp(self, timestamp_ms: int):
        """Set timestamp of current run in milliseconds."""
        self.db.set_setting('last_run_timestamp', str(timestamp_ms))

    def log_event(self, event_type: str, details: Dict = None):
        """Log scraping event"""
        self.db.log_scrape_event(event_type, details)

        # Analyze and adapt on problems
        if event_type in ["rate_limit", "block"]:
            self.analyze_and_adapt()

    def analyze_and_adapt(self):
        """Analyze recent events and adapt strategy"""
        stats = self.db.get_scrape_stats(hours=24)

        total = sum(stats.values())
        if total < 5:
            return

        successes = stats.get('success', 0)
        blocks = stats.get('block', 0)
        rate_limits = stats.get('rate_limit', 0)

        success_rate = successes / total
        problem_rate = (blocks + rate_limits) / total

        logger.info(f"📊 Analysis - Last 24h: {total} events, {success_rate:.1%} success, {problem_rate:.1%} problems")

        old_multiplier = self.current_multiplier

        if problem_rate > 0.3:
            self.current_multiplier = min(5.0, self.current_multiplier * 1.5)
        elif problem_rate > 0.1:
            self.current_multiplier = min(3.0, self.current_multiplier * 1.2)
        elif problem_rate < 0.05 and success_rate > 0.9:
            self.current_multiplier = max(0.5, self.current_multiplier * 0.9)

        if old_multiplier != self.current_multiplier:
            logger.info(f"🔄 Strategy: multiplier {old_multiplier:.2f} → {self.current_multiplier:.2f}")
            self._save_strategy()

    def get_page_delay(self) -> float:
        """Get adaptive page delay"""
        base_min, base_max = self.base_page_delay
        return random.uniform(
            base_min * self.current_multiplier,
            base_max * self.current_multiplier
        )

    def get_cycle_delay(self) -> int:
        """Get adaptive cycle delay in seconds"""
        base_min, base_max = self.base_cycle_delay
        return random.randint(
            int(base_min * self.current_multiplier * 60),
            int(base_max * self.current_multiplier * 60)
        )


class Yad2Monitor:
    """Main monitor class with all features integrated"""

    def __init__(self):
        logger.info("🚀 Initializing Enhanced Yad2Monitor")

        # Initialize database with persistent storage support
        # get_database() auto-detects PostgreSQL (DATABASE_URL) or SQLite
        self.db = get_database()

        # Initialize components
        self.delay_manager = AdaptiveDelayManager(self.db)
        self.proxy_manager = ProxyManager()
        self.proxy_rotator = ProxyRotator(self.proxy_manager)
        self.analytics = MarketAnalytics(self.db)

        # Telegram bot for multi-user support
        telegram_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID")

        try:
            from telegram_bot import TelegramBot
            self.telegram_bot = TelegramBot(telegram_token, self.db)
            logger.info("✓ Telegram bot initialized (multi-user mode)")
        except Exception as e:
            logger.warning(f"Failed to initialize Telegram bot: {e}")
            self.telegram_bot = None

        self.notifier = NotificationManager(self.db, telegram_bot=self.telegram_bot)

        # Load search URLs
        self.search_urls = self._load_search_urls()

        # User agents for rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        ]

        # Web server thread
        self.web_thread = None

        logger.info("✅ Initialization complete")

    def _load_search_urls(self) -> List[Dict]:
        """Load search URLs from database or environment"""
        urls = self.db.get_search_urls()

        if not urls:
            # Add default URL from environment
            default_url = os.environ.get(
                "YAD2_URL",
                "https://www.yad2.co.il/realestate/rent?topArea=41&area=12&city=8400"
            )
            url_id = self.db.add_search_url("Default Search", default_url)
            urls = [{'id': url_id, 'name': "Default Search", 'url': default_url}]

        logger.info(f"📋 Loaded {len(urls)} search URLs")
        return urls

    def get_headers(self) -> Dict:
        """Get request headers with random user agent"""
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }

    def extract_price(self, text: str) -> Optional[int]:
        """Extract price from text"""
        if not text:
            return None
        text = text.replace(',', '').replace('₪', '').strip()
        numbers = re.findall(r'\d+', text)
        if numbers:
            return int(max(numbers, key=int))
        return None

    def extract_data_updated_at_from_page(self, soup) -> List[int]:
        """Extract all dataUpdatedAt timestamps from page"""
        timestamps = []
        try:
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    matches = re.findall(r'"dataUpdatedAt"\s*:\s*(\d{13})', script.string)
                    for match in matches:
                        timestamps.append(int(match))
        except Exception as e:
            logger.debug(f"Error extracting timestamps: {e}")
        return timestamps

    def is_inside_yad1_listing(self, element) -> bool:
        """Check if element is inside Yad1 (promoted) listing"""
        parent = element.parent
        while parent:
            if parent.name == 'div' and parent.get('class'):
                classes = parent.get('class')
                if 'yad1-listing-data-content_yad1ListingDataContentBox__nWOxH' in classes:
                    return True
            parent = parent.parent
        return False

    def find_apartment_elements(self, soup) -> List:
        """Find valid apartment elements"""
        all_h2 = soup.find_all('h2', attrs={'data-nagish': 'content-section-title'})
        valid = [h2 for h2 in all_h2 if not self.is_inside_yad1_listing(h2)]
        return valid

    def get_apartment_container(self, h2_element):
        """Get the container element for an apartment"""
        parent = h2_element.parent
        depth = 0
        while parent and depth < 10:
            if parent.name in ['article', 'div']:
                if parent.find('a', href=True):
                    return parent
            parent = parent.parent
            depth += 1
        return h2_element.parent if h2_element.parent else h2_element

    def get_apartment_id(self, element) -> Optional[str]:
        """Extract apartment ID from element"""
        link = element.find('a', href=True)
        if link:
            href = link['href']
            m = re.search(r'/realestate/item/([A-Za-z0-9]+)', href)
            if m:
                return m.group(1)
        if element.get('data-id'):
            return element.get('data-id')
        import hashlib
        text_content = element.get_text(strip=True)
        return hashlib.md5(text_content.encode()).hexdigest()[:12]

    def parse_apartment(self, h2_element) -> Optional[Dict]:
        """Parse apartment data from HTML element"""
        try:
            container = self.get_apartment_container(h2_element)
            apt_id = self.get_apartment_id(container)

            if not apt_id:
                return None

            title = h2_element.get_text(strip=True)

            # Extract price
            price = None
            price_text = None
            price_elem = container.find('span', class_='feed-item-price_price__ygoeF')
            if not price_elem:
                price_elem = container.find('span', attrs={'data-testid': 'price'})
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                price = self.extract_price(price_text)

            if not price:
                all_text = container.get_text()
                price = self.extract_price(all_text)

            # Extract link
            link = None
            link_elem = container.find('a', href=True)
            if link_elem:
                link = link_elem['href']
                if not link.startswith('http'):
                    link = f"https://www.yad2.co.il{link}"

            if not link or "/realestate/item/" not in link:
                return None

            # Extract address
            street_address = None
            street_elem = container.find('span', class_='item-data-content_heading__tphH4')
            if street_elem:
                street_address = street_elem.get_text(strip=True)

            # Extract item info (rooms, sqm, floor)
            item_info = None
            rooms = None
            sqm = None
            floor = None
            info_elem = container.find('span', class_='item-data-content_itemInfoLine__AeoPP')
            if info_elem:
                item_info = info_elem.get_text(strip=True)
                # Try to parse rooms/sqm/floor
                parts = item_info.split('·')
                for part in parts:
                    part = part.strip()
                    if 'חדרים' in part or 'חדר' in part:
                        nums = re.findall(r'[\d.]+', part)
                        if nums:
                            rooms = float(nums[0])
                    elif 'מ"ר' in part or 'מטר' in part:
                        nums = re.findall(r'\d+', part)
                        if nums:
                            sqm = int(nums[0])
                    elif 'קומה' in part:
                        nums = re.findall(r'\d+', part)
                        if nums:
                            floor = int(nums[0])

            # Extract dataUpdatedAt
            data_updated_at = None
            container_str = str(container)
            match = re.search(r'"dataUpdatedAt"\s*:\s*(\d{13})', container_str)
            if match:
                data_updated_at = int(match.group(1))

            # Extract image URL
            image_url = None
            img_elem = container.find('img')
            if img_elem:
                image_url = img_elem.get('src') or img_elem.get('data-src')

            return {
                'id': apt_id,
                'title': title or 'No title',
                'price': price,
                'price_text': price_text,
                'location': street_address,
                'street_address': street_address,
                'item_info': item_info,
                'rooms': rooms,
                'sqm': sqm,
                'floor': floor,
                'link': link,
                'image_url': image_url,
                'data_updated_at': data_updated_at,
                'last_seen': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"❌ Error parsing apartment: {e}", exc_info=True)
            return None

    def fetch_page(self, url: str, page: int = 1, max_retries: int = 3) -> Optional[str]:
        """Fetch a page with retry logic and proxy support"""
        for attempt in range(max_retries):
            try:
                delay = self.delay_manager.get_page_delay() * (attempt + 1)
                logger.info(f"⏳ Delay: {delay:.2f}s before page {page}")
                time.sleep(delay)

                if page > 1:
                    separator = '&' if '?' in url else '?'
                    page_url = f"{url}{separator}page={page}"
                else:
                    page_url = url

                logger.info(f"🌐 Fetching page {page}")

                # Use proxy if available
                if self.proxy_manager.proxies:
                    response = self.proxy_rotator.make_request(
                        page_url,
                        headers=self.get_headers(),
                        timeout=30
                    )
                else:
                    response = requests.get(
                        page_url,
                        headers=self.get_headers(),
                        timeout=30
                    )

                if response is None:
                    continue

                if response.status_code == 429:
                    self.delay_manager.log_event("rate_limit", {"page": page})
                    wait = 300 * (attempt + 1) * self.delay_manager.current_multiplier
                    logger.warning(f"⚠️ Rate limited! Waiting {wait // 60:.0f} minutes...")
                    time.sleep(wait)
                    continue

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    block_header = soup.find('h1', class_='title')

                    if block_header and "Are you for real" in block_header.get_text():
                        self.delay_manager.log_event("block", {"page": page, "type": "captcha"})
                        delay_seconds = random.randint(120, 300) * (attempt + 1)
                        logger.warning(f"🚫 Blocked! Waiting {delay_seconds // 60:.0f} minutes...")
                        time.sleep(delay_seconds)
                        continue

                    self.delay_manager.log_event("success", {"page": page})
                    logger.info(f"✅ Page {page} fetched successfully")
                    return response.text

                elif response.status_code >= 500:
                    self.delay_manager.log_event("error", {"page": page, "status": response.status_code})
                    continue

            except requests.exceptions.Timeout:
                self.delay_manager.log_event("timeout", {"page": page})
                continue
            except Exception as e:
                self.delay_manager.log_event("error", {"page": page, "exception": str(e)})
                if attempt < max_retries - 1:
                    continue
                return None

        return None

    def scrape_all_pages(self, base_url: str, max_pages: int = 50) -> Tuple[List[Dict], int]:
        """Scrape pages with smart stop based on consecutive known listings"""
        logger.info(f"🔍 Starting smart scrape from {base_url}")

        CONSECUTIVE_KNOWN_THRESHOLD = 4  # Stop after N consecutive known listings

        current_run_ts = int(datetime.now().timestamp() * 1000)
        all_apartments = []
        pages_saved = 0
        page = 1
        consecutive_known = 0

        logger.info(f"📊 Stop strategy: Will stop after {CONSECUTIVE_KNOWN_THRESHOLD} consecutive known listings")

        while page <= max_pages:
            logger.info(f"{'=' * 50}")
            logger.info(f"📄 Processing page {page} (consecutive known: {consecutive_known}/{CONSECUTIVE_KNOWN_THRESHOLD})")

            html = self.fetch_page(base_url, page)
            if not html:
                break

            soup = BeautifulSoup(html, 'html.parser')
            h2_elements = self.find_apartment_elements(soup)
            if not h2_elements:
                break

            parsed_count = 0
            new_on_page = 0
            known_on_page = 0

            for h2_elem in h2_elements:
                apt = self.parse_apartment(h2_elem)
                if apt and apt['price'] and apt['link']:
                    all_apartments.append(apt)
                    parsed_count += 1

                    # Check if apartment already exists in database
                    existing = self.db.get_apartment(apt['id'])
                    if existing:
                        # Known listing
                        consecutive_known += 1
                        known_on_page += 1

                        # Check if we've hit the threshold
                        if consecutive_known >= CONSECUTIVE_KNOWN_THRESHOLD:
                            pages_saved = max_pages - page
                            logger.info(f"🛑 Smart stop: {consecutive_known} consecutive known listings reached!")
                            logger.info(f"💾 Saved approximately {pages_saved} page requests!")
                            # Update last run timestamp before returning
                            self.delay_manager.set_last_run_timestamp(current_run_ts)
                            logger.info(f"{'=' * 50}")
                            logger.info(f"✅ Scraping complete: {len(all_apartments)} apartments from {page} pages")
                            logger.info(f"📊 Last page: {new_on_page} new, {known_on_page} known")
                            return all_apartments, pages_saved
                    else:
                        # New listing - reset counter
                        consecutive_known = 0
                        new_on_page += 1

            logger.info(f"✅ Page {page}: {parsed_count} apartments ({new_on_page} new, {known_on_page} known)")
            page += 1

        # Update last run timestamp
        self.delay_manager.set_last_run_timestamp(current_run_ts)

        logger.info(f"{'=' * 50}")
        logger.info(f"✅ Scraping complete: {len(all_apartments)} apartments from {page - 1} pages")
        if pages_saved > 0:
            logger.info(f"💾 Pages saved: {pages_saved}")

        return all_apartments, pages_saved

    def process_apartments(self, apartments: List[Dict]) -> Tuple[List[Dict], List[Dict], List[str]]:
        """Process apartments and detect changes"""
        new_apartments = []
        price_changes = []
        active_ids = set()

        for apt in apartments:
            apt_id = apt['id']
            active_ids.add(apt_id)

            # Save to database
            _, is_new = self.db.upsert_apartment(apt)

            if is_new:
                new_apartments.append(apt)
                logger.info(f"🆕 New: {apt_id} - {apt['title'][:40]}")
            else:
                # Check for price change
                existing = self.db.get_apartment(apt_id)
                if existing and existing.get('price') != apt.get('price'):
                    old_price = existing['price']
                    new_price = apt['price']
                    if old_price and new_price:
                        change = new_price - old_price
                        change_pct = (change / old_price) * 100
                        price_changes.append({
                            'apartment': apt,
                            'old_price': old_price,
                            'new_price': new_price,
                            'change': change,
                            'change_pct': change_pct
                        })
                        logger.info(f"💰 Price change: {apt_id} ₪{old_price:,} → ₪{new_price:,}")

        # Mark inactive apartments
        removed = self.db.mark_apartments_inactive(active_ids)

        # Update daily summary
        price_drops = len([p for p in price_changes if p['change'] < 0])
        price_increases = len([p for p in price_changes if p['change'] > 0])
        self.db.update_daily_summary(
            new_apts=len(new_apartments),
            price_drops=price_drops,
            price_increases=price_increases,
            removed=len(removed)
        )

        logger.info(f"📊 Summary - New: {len(new_apartments)}, Price changes: {len(price_changes)}, Removed: {len(removed)}")

        return new_apartments, price_changes, removed

    def send_notifications(self, new_apartments: List[Dict], price_changes: List[Dict]):
        """Send notifications for changes"""
        for apt in new_apartments:
            self.notifier.notify_new_apartment(apt)

        for change in price_changes:
            self.notifier.notify_price_change(
                change['apartment'],
                change['old_price'],
                change['new_price']
            )

    def start_web_server(self, port: int = 5000):
        """Start web server in background thread"""
        def run():
            run_web_server(self.db, self.analytics, self.telegram_bot, port=port, debug=False)

        self.web_thread = threading.Thread(target=run, daemon=True)
        self.web_thread.start()
        logger.info(f"🌐 Web server started on port {port}")

        # Set Telegram webhook if bot is configured
        if self.telegram_bot:
            webhook_url = os.environ.get('TELEGRAM_WEBHOOK_URL')
            if webhook_url:
                try:
                    success = self.telegram_bot.set_webhook(f"{webhook_url}/telegram/webhook")
                    if success:
                        logger.info(f"✓ Telegram webhook set: {webhook_url}/telegram/webhook")
                    else:
                        logger.warning("Failed to set Telegram webhook")
                except Exception as e:
                    logger.error(f"Error setting Telegram webhook: {e}", exc_info=True)
            else:
                logger.info("TELEGRAM_WEBHOOK_URL not set - webhook not configured (use polling or set webhook manually)")

    def run_once(self):
        """Run a single scrape cycle"""
        all_new = []
        all_changes = []

        for search in self.search_urls:
            logger.info(f"📋 Scraping: {search['name']}")
            apartments, pages_saved = self.scrape_all_pages(search['url'])

            if apartments:
                new_apts, price_changes, _ = self.process_apartments(apartments)
                all_new.extend(new_apts)
                all_changes.extend(price_changes)

                # Update search URL last scraped
                self.db.update_search_url_scraped(search['id'])

        if all_new or all_changes:
            self.send_notifications(all_new, all_changes)

        # Check for daily digest time
        self.notifier.check_daily_digest_time()

        return len(all_new), len(all_changes)

    def monitor(self):
        """Main monitoring loop"""
        logger.info("=" * 80)
        logger.info("🚀 Starting Yad2 Monitor - Enhanced Edition")
        logger.info("=" * 80)

        # Start web server (Railway sets PORT automatically)
        web_port = int(os.environ.get('PORT', os.environ.get('WEB_PORT', 5000)))
        enable_web = os.environ.get('ENABLE_WEB', 'true').lower() == 'true'
        if enable_web:
            self.start_web_server(web_port)

        # Send startup notification
        self.notifier.send_startup_message({
            'min_interval': int(os.environ.get('MIN_INTERVAL_MINUTES', 60)),
            'max_interval': int(os.environ.get('MAX_INTERVAL_MINUTES', 90))
        })

        iteration = 0

        while True:
            try:
                iteration += 1
                logger.info("=" * 80)
                logger.info(f"🔄 ITERATION {iteration} - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
                logger.info("=" * 80)

                new_count, change_count = self.run_once()

                logger.info(f"✅ Cycle complete - New: {new_count}, Changes: {change_count}")

                # Status report every 10 iterations
                if iteration % 10 == 0:
                    stats = self.db.get_scrape_stats(hours=24)
                    self.notifier.send_status_report(stats, self.delay_manager.current_multiplier)

                # Wait for next cycle
                interval = self.delay_manager.get_cycle_delay()
                next_check = datetime.now() + timedelta(seconds=interval)
                logger.info(f"⏰ Next check: {next_check.strftime('%H:%M:%S')}")
                logger.info(f"😴 Sleeping {interval // 60} minutes...")

                time.sleep(interval)

            except KeyboardInterrupt:
                logger.info("🛑 Stopping monitor...")
                self.notifier.send_telegram_message("🛑 <b>Yad2 Monitor Stopped</b>")
                break
            except Exception as e:
                logger.error(f"❌ Error: {e}", exc_info=True)
                self.delay_manager.log_event("error", {"type": "monitor_loop", "exception": str(e)})
                self.notifier.send_error_alert(str(e), "Monitor loop")
                time.sleep(300)


def main():
    """Main entry point"""
    logger.info("🚀 Starting Yad2 Monitor - Enhanced Edition")

    # Validate environment
    if not os.environ.get("TELEGRAM_BOT_TOKEN"):
        logger.error("❌ TELEGRAM_BOT_TOKEN required")
        exit(1)
    if not os.environ.get("TELEGRAM_CHAT_ID"):
        logger.error("❌ TELEGRAM_CHAT_ID required")
        exit(1)

    monitor = Yad2Monitor()
    monitor.monitor()


if __name__ == "__main__":
    main()
