import requests
from bs4 import BeautifulSoup
import time
import json
import os
from datetime import datetime, timedelta
import hashlib
import random
from typing import Dict, List, Optional, Tuple
import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('yad2_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AdaptiveDelayManager:
    """Analyzes historical scraping data and adapts delays to avoid blocks."""

    def __init__(self, history_file: str = "scrape_history.json"):
        self.history_file = history_file
        self.history = self.load_history()
        self.base_page_delay = (5, 15)
        self.base_cycle_delay = (60, 90)
        self.current_multiplier = 1.0
        self.analyze_and_adapt()

    def load_history(self) -> Dict:
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading scrape history: {e}")
        return {
            "events": [],
            "daily_stats": {},
            "hourly_stats": {},
            "current_strategy": {
                "page_delay_multiplier": 1.0,
                "cycle_delay_multiplier": 1.0,
                "last_updated": None,
                "reason": "Initial settings"
            },
            "last_run_timestamp": None  # Track last successful run
        }

    def save_history(self):
        try:
            if len(self.history["events"]) > 1000:
                self.history["events"] = self.history["events"][-1000:]
            with open(self.history_file, 'w') as f:
                json.dump(self.history, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Error saving scrape history: {e}")

    def get_last_run_timestamp(self) -> Optional[int]:
        """Get timestamp of last successful run in milliseconds."""
        return self.history.get("last_run_timestamp")

    def set_last_run_timestamp(self, timestamp_ms: int):
        """Set timestamp of current run in milliseconds."""
        self.history["last_run_timestamp"] = timestamp_ms
        self.save_history()

    def log_event(self, event_type: str, details: Dict = None):
        now = datetime.now()
        event = {
            "timestamp": now.isoformat(),
            "type": event_type,
            "hour": now.hour,
            "weekday": now.weekday(),
            "details": details or {}
        }
        self.history["events"].append(event)

        date_key = now.strftime("%Y-%m-%d")
        if date_key not in self.history["daily_stats"]:
            self.history["daily_stats"][date_key] = {
                "success": 0, "rate_limit": 0, "block": 0, "timeout": 0, "error": 0
            }
        if event_type in self.history["daily_stats"][date_key]:
            self.history["daily_stats"][date_key][event_type] += 1

        hour_key = str(now.hour)
        if hour_key not in self.history["hourly_stats"]:
            self.history["hourly_stats"][hour_key] = {
                "success": 0, "rate_limit": 0, "block": 0, "timeout": 0, "error": 0
            }
        if event_type in self.history["hourly_stats"][hour_key]:
            self.history["hourly_stats"][hour_key][event_type] += 1

        self.save_history()

        if event_type in ["rate_limit", "block"]:
            self.analyze_and_adapt()

    def analyze_and_adapt(self):
        events = self.history["events"]
        if len(events) < 5:
            logger.info("📊 Not enough data for analysis yet")
            return

        cutoff = datetime.now() - timedelta(hours=24)
        recent_events = [
            e for e in events
            if datetime.fromisoformat(e["timestamp"]) > cutoff
        ]

        if not recent_events:
            return

        total = len(recent_events)
        successes = sum(1 for e in recent_events if e["type"] == "success")
        blocks = sum(1 for e in recent_events if e["type"] == "block")
        rate_limits = sum(1 for e in recent_events if e["type"] == "rate_limit")

        success_rate = successes / total if total > 0 else 1.0
        problem_rate = (blocks + rate_limits) / total if total > 0 else 0.0

        logger.info(f"📊 Analysis - Last 24h: {total} events, {success_rate:.1%} success, {problem_rate:.1%} problems")

        old_multiplier = self.current_multiplier
        reason = ""

        if problem_rate > 0.3:
            self.current_multiplier = min(5.0, self.current_multiplier * 1.5)
            reason = f"High problem rate ({problem_rate:.1%}) - increasing delays"
        elif problem_rate > 0.1:
            self.current_multiplier = min(3.0, self.current_multiplier * 1.2)
            reason = f"Moderate problem rate ({problem_rate:.1%}) - slightly increasing delays"
        elif problem_rate < 0.05 and success_rate > 0.9:
            self.current_multiplier = max(0.5, self.current_multiplier * 0.9)
            reason = f"Good performance ({success_rate:.1%} success) - optimizing delays"
        else:
            reason = "Maintaining current strategy"

        risky_hours = self.find_risky_hours()
        if risky_hours:
            logger.info(f"⚠️ Risky hours detected: {risky_hours}")

        self.history["current_strategy"] = {
            "page_delay_multiplier": self.current_multiplier,
            "cycle_delay_multiplier": self.current_multiplier,
            "last_updated": datetime.now().isoformat(),
            "reason": reason,
            "success_rate": success_rate,
            "problem_rate": problem_rate,
            "risky_hours": risky_hours
        }

        if old_multiplier != self.current_multiplier:
            logger.info(f"🔄 Strategy updated: multiplier {old_multiplier:.2f} → {self.current_multiplier:.2f}")
            logger.info(f"📝 Reason: {reason}")

        self.save_history()

    def find_risky_hours(self) -> List[int]:
        risky = []
        for hour, stats in self.history["hourly_stats"].items():
            total = sum(stats.values())
            if total < 3:
                continue
            problems = stats.get("block", 0) + stats.get("rate_limit", 0)
            if problems / total > 0.2:
                risky.append(int(hour))
        return sorted(risky)

    def get_page_delay(self) -> float:
        base_min, base_max = self.base_page_delay
        adjusted_min = base_min * self.current_multiplier
        adjusted_max = base_max * self.current_multiplier

        current_hour = datetime.now().hour
        risky_hours = self.history["current_strategy"].get("risky_hours", [])
        if current_hour in risky_hours:
            adjusted_min *= 1.5
            adjusted_max *= 1.5
            logger.info(f"⚠️ Risky hour ({current_hour}:00) - using extended delays")

        return random.uniform(adjusted_min, adjusted_max)

    def get_cycle_delay(self) -> int:
        base_min, base_max = self.base_cycle_delay
        adjusted_min = int(base_min * self.current_multiplier * 60)
        adjusted_max = int(base_max * self.current_multiplier * 60)

        current_hour = datetime.now().hour
        risky_hours = self.history["current_strategy"].get("risky_hours", [])
        if current_hour in risky_hours:
            adjusted_min = int(adjusted_min * 1.5)
            adjusted_max = int(adjusted_max * 1.5)

        return random.randint(adjusted_min, adjusted_max)

    def get_status_report(self) -> str:
        strategy = self.history.get("current_strategy", {})
        events = self.history.get("events", [])

        cutoff = datetime.now() - timedelta(hours=24)
        recent = [e for e in events if datetime.fromisoformat(e["timestamp"]) > cutoff]

        total = len(recent)
        if total == 0:
            return "📊 No scraping data in last 24h"

        successes = sum(1 for e in recent if e["type"] == "success")
        blocks = sum(1 for e in recent if e["type"] == "block")
        rate_limits = sum(1 for e in recent if e["type"] == "rate_limit")

        # Pages saved info
        pages_saved = self.history.get("pages_saved_total", 0)

        report = (
            f"📊 <b>Adaptive Scraper Status</b>\n"
            f"{'─' * 25}\n\n"
            f"📈 <b>Last 24 Hours:</b>\n"
            f"  ✅ Successes: {successes}\n"
            f"  🚫 Blocks: {blocks}\n"
            f"  ⏳ Rate limits: {rate_limits}\n"
            f"  📊 Success rate: {successes/total:.1%}\n\n"
            f"⚙️ <b>Current Strategy:</b>\n"
            f"  🔄 Delay multiplier: {strategy.get('page_delay_multiplier', 1.0):.2f}x\n"
            f"  📝 Reason: {strategy.get('reason', 'N/A')}\n"
            f"  💾 Pages saved (smart stop): {pages_saved}\n"
        )

        risky_hours = strategy.get("risky_hours", [])
        if risky_hours:
            report += f"  ⚠️ Risky hours: {', '.join(f'{h}:00' for h in risky_hours)}\n"

        return report


class Yad2Monitor:
    def __init__(self, telegram_bot_token: str, telegram_chat_id: str, min_interval_minutes: int = 10, max_interval_minutes: int = 30):
        logger.info("🚀 Initializing Yad2Monitor")
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.min_interval = min_interval_minutes * 60
        self.max_interval = max_interval_minutes * 60
        self.data_file = "yad2_data.json"
        self.price_history_file = "price_history.json"
        self.apartments = {}
        self.price_history = {}
        self.current_check_apartments = set()
        self.use_parallel_messages = True

        self.delay_manager = AdaptiveDelayManager()

        logger.info(f"🔑 Telegram Bot Token: {self.telegram_bot_token[:20]}...")
        logger.info(f"💬 Telegram Chat ID: {self.telegram_chat_id}")
        logger.info(f"⏱️  Base interval range: {min_interval_minutes}-{max_interval_minutes} minutes")
        logger.info(f"🔄 Current delay multiplier: {self.delay_manager.current_multiplier:.2f}x")

        logger.info("🔗 Testing Telegram connection...")
        self.test_telegram_connection()

        self.load_data()

        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1',
        ]
        logger.info("✅ Initialization complete")

    def get_random_interval(self) -> int:
        interval = self.delay_manager.get_cycle_delay()
        logger.info(f"🎲 Adaptive interval: {interval // 60} minutes ({interval} seconds)")
        return interval

    def test_telegram_connection(self):
        try:
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/getMe"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                bot_info = response.json()
                if bot_info.get('ok'):
                    logger.info(f"✅ Bot token is valid. Bot name: @{bot_info['result'].get('username')}")
        except Exception as e:
            logger.error(f"❌ Error testing Telegram connection: {e}")

    def get_headers(self) -> Dict:
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

    def load_data(self):
        logger.info("📂 Loading data from files...")
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    self.apartments = json.load(f)
                logger.info(f"✅ Loaded {len(self.apartments)} apartments from {self.data_file}")
            except Exception as e:
                logger.error(f"❌ Error loading apartments data: {e}")

        if os.path.exists(self.price_history_file):
            try:
                with open(self.price_history_file, 'r', encoding='utf-8') as f:
                    self.price_history = json.load(f)
                logger.info(f"✅ Loaded price history for {len(self.price_history)} apartments")
            except Exception as e:
                logger.error(f"❌ Error loading price history: {e}")

    def save_data(self):
        logger.info("💾 Saving data to files...")
        try:
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(self.apartments, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ Saved {len(self.apartments)} apartments to {self.data_file}")
        except Exception as e:
            logger.error(f"❌ Error saving apartments data: {e}")

        try:
            with open(self.price_history_file, 'w', encoding='utf-8') as f:
                json.dump(self.price_history, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ Saved price history to {self.price_history_file}")
        except Exception as e:
            logger.error(f"❌ Error saving price history: {e}")

    def extract_price(self, text: str) -> Optional[int]:
        if not text:
            return None
        text = text.replace(',', '').replace('₪', '').strip()
        numbers = re.findall(r'\d+', text)
        if numbers:
            return int(max(numbers, key=int))
        return None

    def extract_data_updated_at(self, container) -> Optional[int]:
        """Extract dataUpdatedAt timestamp (in milliseconds) from apartment container."""
        try:
            # Try to find it in data attributes
            for attr in ['data-updated-at', 'data-updatedat', 'dataupdatedat']:
                if container.get(attr):
                    return int(container.get(attr))

            # Try to find in parent elements
            parent = container
            for _ in range(5):
                if parent is None:
                    break
                for attr in ['data-updated-at', 'data-updatedat']:
                    val = parent.get(attr)
                    if val:
                        return int(val)
                parent = parent.parent

            # Try to find in script tags with JSON data
            scripts = container.find_all('script', type='application/json')
            for script in scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and 'dataUpdatedAt' in data:
                        return int(data['dataUpdatedAt'])
                except:
                    pass

            # Search in container's text for timestamp pattern
            container_str = str(container)
            # Look for dataUpdatedAt in JSON-like patterns
            match = re.search(r'"dataUpdatedAt"\s*:\s*(\d{13})', container_str)
            if match:
                return int(match.group(1))

            # Also try without quotes
            match = re.search(r'dataUpdatedAt["\s:]+(\d{13})', container_str)
            if match:
                return int(match.group(1))

        except Exception as e:
            logger.debug(f"Could not extract dataUpdatedAt: {e}")

        return None

    def extract_data_updated_at_from_page(self, soup) -> List[int]:
        """Extract all dataUpdatedAt timestamps from the page's JSON data."""
        timestamps = []
        try:
            # Look for Next.js data or similar JSON embedded in page
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    # Find all dataUpdatedAt values
                    matches = re.findall(r'"dataUpdatedAt"\s*:\s*(\d{13})', script.string)
                    for match in matches:
                        timestamps.append(int(match))
        except Exception as e:
            logger.debug(f"Error extracting timestamps from page: {e}")

        return timestamps

    def is_inside_yad1_listing(self, element) -> bool:
        parent = element.parent
        while parent:
            if parent.name == 'div' and parent.get('class'):
                classes = parent.get('class')
                if 'yad1-listing-data-content_yad1ListingDataContentBox__nWOxH' in classes:
                    return True
            parent = parent.parent
        return False

    def find_apartment_elements(self, soup) -> List:
        all_h2_elements = soup.find_all('h2', attrs={'data-nagish': 'content-section-title'})
        valid_elements = [h2 for h2 in all_h2_elements if not self.is_inside_yad1_listing(h2)]
        logger.info(f"🔍 Found {len(valid_elements)} valid apartment elements")
        return valid_elements

    def get_apartment_container(self, h2_element):
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
        link = element.find('a', href=True)
        if link:
            href = link['href']
            m = re.search(r'/realestate/item/([A-Za-z0-9]+)', href)
            if m:
                return m.group(1)
        if element.get('data-id'):
            return element.get('data-id')
        text_content = element.get_text(strip=True)
        return hashlib.md5(text_content.encode()).hexdigest()[:12]

    def is_valid_apartment_link(self, link: str) -> bool:
        return link and "/realestate/item/" in link

    def parse_apartment(self, h2_element, page_timestamps: List[int] = None) -> Optional[Dict]:
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

            if not link or not self.is_valid_apartment_link(link):
                return None

            # Extract address
            street_address = None
            street_elem = container.find('span', class_='item-data-content_heading__tphH4')
            if street_elem:
                street_address = street_elem.get_text(strip=True)

            # Extract item info
            item_info = None
            info_elem = container.find('span', class_='item-data-content_itemInfoLine__AeoPP')
            if info_elem:
                item_info = info_elem.get_text(strip=True)

            # Extract dataUpdatedAt
            data_updated_at = self.extract_data_updated_at(container)

            return {
                'id': apt_id,
                'title': title or 'No title',
                'price': price,
                'price_text': price_text,
                'location': street_address,
                'street_address': street_address,
                'item_info': item_info,
                'link': link,
                'data_updated_at': data_updated_at,
                'last_seen': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"❌ Error parsing apartment: {e}", exc_info=True)
            return None

    def fetch_page(self, url: str, page: int = 1, max_retries: int = 3) -> Optional[str]:
        for attempt in range(max_retries):
            try:
                delay = self.delay_manager.get_page_delay() * (attempt + 1)
                logger.info(f"⏳ Adaptive delay: {delay:.2f}s before page {page}")
                time.sleep(delay)

                if page > 1:
                    separator = '&' if '?' in url else '?'
                    page_url = f"{url}{separator}page={page}"
                else:
                    page_url = url

                logger.info(f"🌐 Fetching page {page}")
                response = requests.get(page_url, headers=self.get_headers(), timeout=30)

                if response.status_code == 429:
                    self.delay_manager.log_event("rate_limit", {"page": page})
                    wait_time = 300 * (attempt + 1) * self.delay_manager.current_multiplier
                    logger.warning(f"⚠️ Rate limited! Waiting {wait_time // 60:.0f} minutes...")
                    time.sleep(wait_time)
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
                else:
                    return None

            except requests.exceptions.Timeout:
                self.delay_manager.log_event("timeout", {"page": page})
                continue
            except requests.exceptions.ConnectionError:
                self.delay_manager.log_event("error", {"page": page, "type": "connection"})
                time.sleep(30 * (attempt + 1))
                continue
            except Exception as e:
                self.delay_manager.log_event("error", {"page": page, "exception": str(e)})
                if attempt < max_retries - 1:
                    continue
                return None

        return None

    def scrape_all_pages(self, base_url: str, max_pages: int = 50) -> Tuple[List[Dict], int]:
        """
        Scrape pages with smart stop - stops when hitting apartments older than last run.
        Returns tuple of (apartments_list, pages_saved_count)
        """
        logger.info(f"🔍 Starting smart scrape from {base_url}")

        last_run_ts = self.delay_manager.get_last_run_timestamp()
        if last_run_ts:
            last_run_dt = datetime.fromtimestamp(last_run_ts / 1000)
            logger.info(f"📅 Last run: {last_run_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            logger.info("📅 First run - will scrape all pages")

        # Record current run start time
        current_run_ts = int(datetime.now().timestamp() * 1000)

        all_apartments = []
        pages_saved = 0
        stop_reason = None
        oldest_timestamp_seen = None

        page = 1
        while page <= max_pages:
            logger.info(f"{'=' * 50}")
            logger.info(f"📄 Processing page {page}")

            html = self.fetch_page(base_url, page)

            if not html:
                stop_reason = "no_content"
                break

            soup = BeautifulSoup(html, 'html.parser')

            # Extract all timestamps from the page first
            page_timestamps = self.extract_data_updated_at_from_page(soup)
            if page_timestamps:
                min_ts = min(page_timestamps)
                max_ts = max(page_timestamps)
                logger.info(f"📊 Page timestamps range: {datetime.fromtimestamp(min_ts/1000).strftime('%H:%M:%S')} - {datetime.fromtimestamp(max_ts/1000).strftime('%H:%M:%S')}")

                if oldest_timestamp_seen is None or min_ts < oldest_timestamp_seen:
                    oldest_timestamp_seen = min_ts

                # Smart stop: if the newest item on this page is older than last run, stop
                if last_run_ts and max_ts < last_run_ts:
                    pages_saved = max_pages - page
                    stop_reason = "smart_stop"
                    logger.info(f"🛑 Smart stop: Page {page} has no new updates (newest: {datetime.fromtimestamp(max_ts/1000).strftime('%H:%M:%S')})")
                    logger.info(f"💾 Saved {pages_saved} page requests!")
                    break

            h2_elements = self.find_apartment_elements(soup)

            if not h2_elements:
                stop_reason = "no_apartments"
                break

            parsed_count = 0
            found_old_apartment = False

            for h2_elem in h2_elements:
                apt = self.parse_apartment(h2_elem, page_timestamps)

                if apt and apt['price'] and apt['link']:
                    all_apartments.append(apt)
                    parsed_count += 1

                    # Check if this apartment is older than last run
                    if apt.get('data_updated_at') and last_run_ts:
                        if apt['data_updated_at'] < last_run_ts:
                            found_old_apartment = True

            logger.info(f"✅ Page {page}: parsed {parsed_count} apartments")

            # If we found an old apartment and we're past page 1, we can consider stopping
            # But continue for at least this page to get all new ones
            if found_old_apartment and page > 1:
                # Check if ALL remaining apartments are old
                new_on_page = sum(1 for apt in all_apartments[-parsed_count:]
                                  if apt.get('data_updated_at') and apt['data_updated_at'] >= last_run_ts)
                if new_on_page == 0:
                    pages_saved = max_pages - page
                    stop_reason = "all_old"
                    logger.info(f"🛑 Smart stop: All apartments on page {page} are old")
                    break

            page += 1

        # Update last run timestamp
        self.delay_manager.set_last_run_timestamp(current_run_ts)

        # Track pages saved stats
        if pages_saved > 0:
            total_saved = self.delay_manager.history.get("pages_saved_total", 0) + pages_saved
            self.delay_manager.history["pages_saved_total"] = total_saved
            self.delay_manager.save_history()

        logger.info(f"{'=' * 50}")
        logger.info(f"✅ Scraping complete: {len(all_apartments)} apartments from {page - 1} pages")
        if stop_reason:
            logger.info(f"📝 Stop reason: {stop_reason}")
        if pages_saved > 0:
            logger.info(f"💾 Pages saved this run: {pages_saved}")

        return all_apartments, pages_saved

    def send_telegram_message(self, message: str, max_retries: int = 3) -> bool:
        for attempt in range(max_retries):
            try:
                url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
                data = {
                    'chat_id': self.telegram_chat_id,
                    'text': message,
                    'parse_mode': 'HTML',
                    'disable_web_page_preview': False
                }

                response = requests.post(url, data=data, timeout=10)

                if response.status_code == 200:
                    return True
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 30))
                    time.sleep(retry_after)
                    continue
                elif response.status_code >= 500:
                    time.sleep(5 * (attempt + 1))
                    continue
                else:
                    return False

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                return False

        return False

    def update_price_history(self, apt_id: str, price: int):
        if apt_id not in self.price_history:
            self.price_history[apt_id] = []
        self.price_history[apt_id].append({
            'price': price,
            'timestamp': datetime.now().isoformat()
        })
        self.price_history[apt_id] = self.price_history[apt_id][-50:]

    def get_last_price_change_date(self, apt_id: str) -> Optional[str]:
        if apt_id not in self.price_history or len(self.price_history[apt_id]) < 2:
            return None
        return self.price_history[apt_id][-2]['timestamp']

    def check_for_changes(self, new_apartments: List[Dict]):
        logger.info(f"🔍 Checking for changes in {len(new_apartments)} apartments")

        self.current_check_apartments = set()
        new_apartments_list = []
        price_changes_list = []

        for apt in new_apartments:
            apt_id = apt['id']
            current_price = apt['price']
            self.current_check_apartments.add(apt_id)

            if apt_id in self.apartments:
                old_price = self.apartments[apt_id].get('price')

                if old_price and current_price and old_price != current_price:
                    change = current_price - old_price
                    change_pct = (change / old_price) * 100

                    logger.info(f"💰 Price change: {apt_id} ₪{old_price:,} → ₪{current_price:,}")

                    old_price_date = self.get_last_price_change_date(apt_id)
                    if not old_price_date:
                        old_price_date = self.apartments[apt_id].get('last_seen', datetime.now().isoformat())

                    price_changes_list.append({
                        'apartment': apt,
                        'old_price': old_price,
                        'new_price': current_price,
                        'change': change,
                        'change_pct': change_pct,
                        'old_price_date': old_price_date,
                        'new_price_date': apt['last_seen']
                    })

                    self.update_price_history(apt_id, current_price)
            else:
                logger.info(f"🆕 New apartment: {apt_id} - {apt['title'][:40]}")
                new_apartments_list.append(apt)
                self.update_price_history(apt_id, current_price)

            self.apartments[apt_id] = apt

        removed = set(self.apartments.keys()) - self.current_check_apartments
        for apt_id in removed:
            logger.info(f"🗑️ Removed: {apt_id}")
            del self.apartments[apt_id]

        logger.info(f"📊 Summary - New: {len(new_apartments_list)}, Price changes: {len(price_changes_list)}, Removed: {len(removed)}")

        if new_apartments_list or price_changes_list:
            self.send_batch_telegram_messages(new_apartments_list, price_changes_list)

    def send_single_telegram_message(self, message: str):
        try:
            self.send_telegram_message(message)
            time.sleep(0.5)
            return True
        except:
            return False

    def send_batch_telegram_messages(self, new_apartments_list: List[Dict], price_changes_list: List[Dict]):
        messages = []

        for apt in new_apartments_list:
            timestamp = datetime.fromisoformat(apt['last_seen']).strftime('%d/%m/%Y %H:%M')
            info_line = f"📋 {apt['item_info']}\n" if apt.get('item_info') else ''

            message = (
                f"🆕 <b>דירה חדשה!</b>\n"
                f"{'─' * 30}\n\n"
                f"<b>📍 {apt['title']}</b>\n\n"
                f"🏠 <b>כתובת:</b> {apt.get('street_address') or 'לא צוין'}\n"
                f"{info_line}"
                f"💰 <b>מחיר:</b> ₪{apt['price']:,}\n"
                f"📅 <b>תאריך:</b> {timestamp}\n\n"
                f"🔗 <a href='{apt['link']}'>לצפייה בדירה</a>"
            )
            messages.append(message)

        for change_info in price_changes_list:
            apt = change_info['apartment']
            old_price = change_info['old_price']
            new_price = change_info['new_price']
            change = change_info['change']
            change_pct = change_info['change_pct']

            emoji = "📉" if change < 0 else "📈"
            change_text = "ירידת מחיר!" if change < 0 else "עליית מחיר"

            message = (
                f"{emoji} <b>{change_text}</b>\n"
                f"{'─' * 30}\n\n"
                f"<b>📍 {apt['title']}</b>\n\n"
                f"💵 <b>מחיר קודם:</b> ₪{old_price:,}\n"
                f"💰 <b>מחיר חדש:</b> ₪{new_price:,}\n"
                f"{'🔽' if change < 0 else '🔼'} <b>שינוי:</b> ₪{abs(change):,} ({change_pct:+.1f}%)\n\n"
                f"🔗 <a href='{apt['link']}'>לצפייה בדירה</a>"
            )
            messages.append(message)

        if self.use_parallel_messages:
            try:
                with ThreadPoolExecutor(max_workers=5) as executor:
                    list(executor.map(self.send_single_telegram_message, messages))
                return
            except:
                self.use_parallel_messages = False

        for msg in messages:
            self.send_single_telegram_message(msg)

    def monitor(self, url: str):
        logger.info("=" * 80)
        logger.info("🚀 Starting Yad2 Monitor with Smart Pagination")
        logger.info("=" * 80)

        self.send_telegram_message(
            "🤖 <b>Yad2 Monitor Started!</b>\n\n"
            f"🔄 <b>Adaptive System Active</b>\n"
            f"📊 Delay multiplier: {self.delay_manager.current_multiplier:.2f}x\n"
            f"🧠 <b>Smart pagination:</b> Stops when no new updates\n\n"
            "🔍 <b>Status:</b> Active and monitoring..."
        )

        iteration = 0

        while True:
            try:
                iteration += 1
                logger.info("=" * 80)
                logger.info(f"🔄 ITERATION {iteration} - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
                logger.info("=" * 80)

                apartments, pages_saved = self.scrape_all_pages(url)

                if apartments:
                    logger.info(f"✅ Found {len(apartments)} apartments")

                    self.check_for_changes(apartments)
                    self.save_data()

                    if pages_saved > 0:
                        logger.info(f"💾 Smart stop saved {pages_saved} page requests!")

                # Status report every 10 iterations
                if iteration % 10 == 0:
                    self.send_telegram_message(self.delay_manager.get_status_report())

                interval = self.get_random_interval()
                next_check = datetime.now() + timedelta(seconds=interval)
                logger.info(f"⏰ Next check: {next_check.strftime('%H:%M:%S')}")
                logger.info(f"😴 Sleeping {interval // 60} minutes...")

                sleep_start = time.time()
                while time.time() - sleep_start < interval:
                    time.sleep(min(60, interval - (time.time() - sleep_start)))

            except KeyboardInterrupt:
                logger.info("🛑 Stopping monitor...")
                self.send_telegram_message("🛑 <b>Yad2 Monitor Stopped</b>")
                break
            except Exception as e:
                logger.error(f"❌ Error: {e}", exc_info=True)
                self.delay_manager.log_event("error", {"type": "monitor_loop", "exception": str(e)})
                self.send_telegram_message(f"❌ <b>Error</b>\n\n<code>{str(e)}</code>")
                time.sleep(300)


if __name__ == "__main__":
    logger.info("🚀 Starting Yad2 Monitor with Smart Pagination")

    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
    YAD2_URL = os.environ.get("YAD2_URL", "https://www.yad2.co.il/realestate/rent?minRooms=4&minSqarremeter=100&topArea=41&area=12&city=8400")
    MIN_INTERVAL_MINUTES = int(os.environ.get("MIN_INTERVAL_MINUTES", "60"))
    MAX_INTERVAL_MINUTES = int(os.environ.get("MAX_INTERVAL_MINUTES", "90"))

    if not TELEGRAM_BOT_TOKEN:
        logger.error("❌ TELEGRAM_BOT_TOKEN required")
        exit(1)
    if not TELEGRAM_CHAT_ID:
        logger.error("❌ TELEGRAM_CHAT_ID required")
        exit(1)

    logger.info(f"⚙️ Config: Interval {MIN_INTERVAL_MINUTES}-{MAX_INTERVAL_MINUTES} min")

    monitor = Yad2Monitor(
        telegram_bot_token=TELEGRAM_BOT_TOKEN,
        telegram_chat_id=TELEGRAM_CHAT_ID,
        min_interval_minutes=MIN_INTERVAL_MINUTES,
        max_interval_minutes=MAX_INTERVAL_MINUTES
    )

    monitor.monitor(YAD2_URL)
