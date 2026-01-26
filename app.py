import requests
from bs4 import BeautifulSoup
import time
import json
import os
from datetime import datetime, timedelta
import hashlib
import random
from typing import Dict, List, Optional
import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

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

        # Default settings
        self.base_page_delay = (5, 15)  # min, max seconds between pages
        self.base_cycle_delay = (60, 90)  # min, max minutes between cycles
        self.current_multiplier = 1.0

        # Analyze history on startup
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
            }
        }

    def save_history(self):
        try:
            # Keep only last 1000 events to prevent file bloat
            if len(self.history["events"]) > 1000:
                self.history["events"] = self.history["events"][-1000:]

            with open(self.history_file, 'w') as f:
                json.dump(self.history, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Error saving scrape history: {e}")

    def log_event(self, event_type: str, details: Dict = None):
        """Log a scraping event for future analysis."""
        now = datetime.now()
        event = {
            "timestamp": now.isoformat(),
            "type": event_type,  # success, rate_limit, block, timeout, error
            "hour": now.hour,
            "weekday": now.weekday(),
            "details": details or {}
        }
        self.history["events"].append(event)

        # Update daily stats
        date_key = now.strftime("%Y-%m-%d")
        if date_key not in self.history["daily_stats"]:
            self.history["daily_stats"][date_key] = {
                "success": 0, "rate_limit": 0, "block": 0, "timeout": 0, "error": 0
            }
        if event_type in self.history["daily_stats"][date_key]:
            self.history["daily_stats"][date_key][event_type] += 1

        # Update hourly stats
        hour_key = str(now.hour)
        if hour_key not in self.history["hourly_stats"]:
            self.history["hourly_stats"][hour_key] = {
                "success": 0, "rate_limit": 0, "block": 0, "timeout": 0, "error": 0
            }
        if event_type in self.history["hourly_stats"][hour_key]:
            self.history["hourly_stats"][hour_key][event_type] += 1

        self.save_history()

        # Re-analyze if we hit a problem
        if event_type in ["rate_limit", "block"]:
            self.analyze_and_adapt()

    def analyze_and_adapt(self):
        """Analyze historical data and adapt scraping strategy."""
        events = self.history["events"]
        if len(events) < 5:
            logger.info("📊 Not enough data for analysis yet")
            return

        # Analyze last 24 hours
        cutoff = datetime.now() - timedelta(hours=24)
        recent_events = [
            e for e in events
            if datetime.fromisoformat(e["timestamp"]) > cutoff
        ]

        if not recent_events:
            return

        # Calculate success rate
        total = len(recent_events)
        successes = sum(1 for e in recent_events if e["type"] == "success")
        blocks = sum(1 for e in recent_events if e["type"] == "block")
        rate_limits = sum(1 for e in recent_events if e["type"] == "rate_limit")

        success_rate = successes / total if total > 0 else 1.0
        problem_rate = (blocks + rate_limits) / total if total > 0 else 0.0

        logger.info(f"📊 Analysis - Last 24h: {total} events, {success_rate:.1%} success, {problem_rate:.1%} problems")

        # Determine new multiplier based on patterns
        old_multiplier = self.current_multiplier
        reason = ""

        if problem_rate > 0.3:
            # High problem rate - significantly increase delays
            self.current_multiplier = min(5.0, self.current_multiplier * 1.5)
            reason = f"High problem rate ({problem_rate:.1%}) - increasing delays"
        elif problem_rate > 0.1:
            # Moderate problems - slightly increase
            self.current_multiplier = min(3.0, self.current_multiplier * 1.2)
            reason = f"Moderate problem rate ({problem_rate:.1%}) - slightly increasing delays"
        elif problem_rate < 0.05 and success_rate > 0.9:
            # Good performance - can decrease delays slightly
            self.current_multiplier = max(0.5, self.current_multiplier * 0.9)
            reason = f"Good performance ({success_rate:.1%} success) - optimizing delays"
        else:
            reason = "Maintaining current strategy"

        # Find risky hours
        risky_hours = self.find_risky_hours()
        if risky_hours:
            logger.info(f"⚠️ Risky hours detected: {risky_hours}")

        # Update strategy
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
        """Find hours with high block/rate-limit rates."""
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
        """Get adaptive delay between pages."""
        base_min, base_max = self.base_page_delay
        adjusted_min = base_min * self.current_multiplier
        adjusted_max = base_max * self.current_multiplier

        # Add extra delay during risky hours
        current_hour = datetime.now().hour
        risky_hours = self.history["current_strategy"].get("risky_hours", [])
        if current_hour in risky_hours:
            adjusted_min *= 1.5
            adjusted_max *= 1.5
            logger.info(f"⚠️ Risky hour ({current_hour}:00) - using extended delays")

        delay = random.uniform(adjusted_min, adjusted_max)
        return delay

    def get_cycle_delay(self) -> int:
        """Get adaptive delay between scraping cycles (in seconds)."""
        base_min, base_max = self.base_cycle_delay
        adjusted_min = int(base_min * self.current_multiplier * 60)
        adjusted_max = int(base_max * self.current_multiplier * 60)

        # Add extra delay during risky hours
        current_hour = datetime.now().hour
        risky_hours = self.history["current_strategy"].get("risky_hours", [])
        if current_hour in risky_hours:
            adjusted_min = int(adjusted_min * 1.5)
            adjusted_max = int(adjusted_max * 1.5)

        return random.randint(adjusted_min, adjusted_max)

    def get_status_report(self) -> str:
        """Generate a status report for Telegram."""
        strategy = self.history.get("current_strategy", {})
        events = self.history.get("events", [])

        # Last 24h stats
        cutoff = datetime.now() - timedelta(hours=24)
        recent = [e for e in events if datetime.fromisoformat(e["timestamp"]) > cutoff]

        total = len(recent)
        if total == 0:
            return "📊 No scraping data in last 24h"

        successes = sum(1 for e in recent if e["type"] == "success")
        blocks = sum(1 for e in recent if e["type"] == "block")
        rate_limits = sum(1 for e in recent if e["type"] == "rate_limit")

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

        # Initialize adaptive delay manager
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
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (iPad; CPU OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1'
        ]
        logger.info("✅ Initialization complete")

    def get_random_interval(self) -> int:
        """Generate adaptive random interval based on historical performance."""
        interval = self.delay_manager.get_cycle_delay()
        logger.info(f"🎲 Adaptive interval: {interval // 60} minutes ({interval} seconds)")
        return interval

    def test_telegram_connection(self):
        try:
            logger.info("🔍 Testing bot token with getMe API...")
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/getMe"
            response = requests.get(url, timeout=10)
            logger.info(f"📊 getMe response status: {response.status_code}")

            if response.status_code == 200:
                bot_info = response.json()
                if bot_info.get('ok'):
                    logger.info(f"✅ Bot token is valid. Bot name: @{bot_info['result'].get('username')}")
                else:
                    logger.error(f"❌ Bot token validation failed: {bot_info}")
            else:
                logger.error(f"❌ Bot token test failed with status {response.status_code}")

        except Exception as e:
            logger.error(f"❌ Error testing Telegram connection: {e}", exc_info=True)

    def get_headers(self) -> Dict:
        selected_agent = random.choice(self.user_agents)
        return {
            'User-Agent': selected_agent,
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
        else:
            logger.info(f"📝 No existing {self.data_file} found, starting fresh")

        if os.path.exists(self.price_history_file):
            try:
                with open(self.price_history_file, 'r', encoding='utf-8') as f:
                    self.price_history = json.load(f)
                logger.info(f"✅ Loaded price history for {len(self.price_history)} apartments")
            except Exception as e:
                logger.error(f"❌ Error loading price history: {e}")
        else:
            logger.info(f"📝 No existing {self.price_history_file} found, starting fresh")

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
            price = int(max(numbers, key=int))
            return price
        return None

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
        logger.info(f"🔍 Found {len(all_h2_elements)} total h2 elements")

        valid_elements = [h2 for h2 in all_h2_elements if not self.is_inside_yad1_listing(h2)]
        logger.info(f"✅ Found {len(valid_elements)} valid apartment h2 elements (excluding yad1 listings)")
        return valid_elements

    def get_apartment_container(self, h2_element):
        parent = h2_element.parent
        depth = 0
        max_depth = 10

        while parent and depth < max_depth:
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
        if not link:
            return False
        return "/realestate/item/" in link

    def parse_apartment(self, h2_element) -> Optional[Dict]:
        try:
            container = self.get_apartment_container(h2_element)
            apt_id = self.get_apartment_id(container)

            if not apt_id:
                logger.warning("⚠️  Could not extract apartment ID")
                return None

            title = h2_element.get_text(strip=True)

            price = None
            price_text = None
            price_elem = container.find('span', class_='feed-item-price_price__ygoeF')
            if not price_elem:
                price_elem = container.find('span', attrs={'data-testid': 'price'})

            if price_elem:
                price_text = price_elem.get_text(strip=True)
                price = self.extract_price(price_text)

            if not price:
                for selector in ['.price', '[class*="price"]', '[class*="Price"]']:
                    price_elem = container.find(class_=re.compile(selector.replace('.', '')))
                    if price_elem:
                        price_text = price_elem.get_text(strip=True)
                        price = self.extract_price(price_text)
                        if price:
                            break

            if not price:
                all_text = container.get_text()
                price = self.extract_price(all_text)

            link = None
            link_elem = container.find('a', href=True)
            if link_elem:
                link = link_elem['href']
                if not link.startswith('http'):
                    link = f"https://www.yad2.co.il{link}"

            if not link or not self.is_valid_apartment_link(link):
                return None

            street_address = None
            street_elem = container.find('span', class_='item-data-content_heading__tphH4')
            if street_elem:
                street_address = street_elem.get_text(strip=True)

            location = street_address
            if not location:
                for selector in ['.location', '[class*="location"]', '[class*="address"]']:
                    loc_elem = container.find(class_=re.compile(selector.replace('.', '')))
                    if loc_elem:
                        location = loc_elem.get_text(strip=True)
                        break

            item_info = None
            info_elem = container.find('span', class_='item-data-content_itemInfoLine__AeoPP')
            if not info_elem:
                info_elem = container.find('span', attrs={'data-testid': 'item-info-line-2nd'})

            if info_elem:
                item_info = info_elem.get_text(strip=True)

            return {
                'id': apt_id,
                'title': title or 'No title',
                'price': price,
                'price_text': price_text,
                'location': location,
                'street_address': street_address,
                'item_info': item_info,
                'link': link,
                'last_seen': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"❌ Error parsing apartment: {e}", exc_info=True)
            return None

    def fetch_page(self, url: str, page: int = 1, max_retries: int = 3) -> Optional[str]:
        for attempt in range(max_retries):
            try:
                # Use adaptive delay
                delay = self.delay_manager.get_page_delay() * (attempt + 1)
                logger.info(f"⏳ Adaptive delay: {delay:.2f}s before page {page} (attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)

                if page > 1:
                    separator = '&' if '?' in url else '?'
                    page_url = f"{url}{separator}page={page}"
                else:
                    page_url = url

                logger.info(f"🌐 Fetching page {page} from URL: {page_url}")
                response = requests.get(page_url, headers=self.get_headers(), timeout=30)
                logger.info(f"📊 Response status code: {response.status_code}")

                if response.status_code == 429:
                    # Log rate limit event
                    self.delay_manager.log_event("rate_limit", {"page": page, "attempt": attempt})

                    wait_time = 300 * (attempt + 1) * self.delay_manager.current_multiplier
                    logger.warning(f"⚠️  Rate limited! Waiting {wait_time // 60:.0f} minutes...")
                    self.send_telegram_message(f"⚠️ <b>Rate Limited</b>\n\nAdaptive delay activated.\nPausing for {wait_time // 60:.0f} minutes...")
                    time.sleep(wait_time)
                    continue

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    block_header = soup.find('h1', class_='title')

                    if block_header and "Are you for real" in block_header.get_text():
                        # Log block event
                        self.delay_manager.log_event("block", {"page": page, "attempt": attempt, "type": "captcha"})

                        logger.warning("🚫 Yad2 suspects scraping! 'Are you for real' page detected.")
                        delay_seconds = random.randint(120, 300) * (attempt + 1) * self.delay_manager.current_multiplier
                        self.send_telegram_message(
                            f"🚫 <b>Scraping Detection</b>\n\n"
                            f"Yad2 suspects bot activity.\n"
                            f"Adaptive system activating extended delay.\n"
                            f"Pausing for {delay_seconds // 60:.0f} minutes..."
                        )
                        time.sleep(delay_seconds)
                        continue

                    # Log success
                    self.delay_manager.log_event("success", {"page": page, "content_length": len(response.text)})
                    logger.info(f"✅ Successfully fetched page {page}, content length: {len(response.text)} bytes")
                    return response.text

                elif response.status_code >= 500:
                    self.delay_manager.log_event("error", {"page": page, "status": response.status_code})
                    logger.warning(f"⚠️ Server error {response.status_code}, retrying...")
                    continue
                else:
                    self.delay_manager.log_event("error", {"page": page, "status": response.status_code})
                    logger.error(f"❌ Error fetching page {page}: Status {response.status_code}")
                    return None

            except requests.exceptions.Timeout:
                self.delay_manager.log_event("timeout", {"page": page, "attempt": attempt})
                logger.warning(f"⚠️ Timeout on attempt {attempt + 1}, retrying...")
                continue
            except requests.exceptions.ConnectionError:
                self.delay_manager.log_event("error", {"page": page, "type": "connection"})
                logger.warning(f"⚠️ Connection error on attempt {attempt + 1}, retrying...")
                time.sleep(30 * (attempt + 1))
                continue
            except Exception as e:
                self.delay_manager.log_event("error", {"page": page, "exception": str(e)})
                logger.error(f"❌ Error fetching page {page}: {e}", exc_info=True)
                if attempt < max_retries - 1:
                    continue
                return None

        logger.error(f"❌ Failed to fetch page {page} after {max_retries} attempts")
        return None

    def scrape_all_pages(self, base_url: str, max_pages: int = 50) -> List[Dict]:
        logger.info(f"🔍 Starting to scrape pages from {base_url}")
        logger.info(f"📄 Max pages: {max_pages}, Delay multiplier: {self.delay_manager.current_multiplier:.2f}x")
        all_apartments = []

        page = 1
        while page <= max_pages:
            logger.info("=" * 60)
            logger.info(f"📄 Processing page {page}")
            logger.info("=" * 60)

            html = self.fetch_page(base_url, page)

            if not html:
                logger.warning(f"⚠️  No HTML content received for page {page}, stopping pagination")
                break

            soup = BeautifulSoup(html, 'html.parser')
            h2_elements = self.find_apartment_elements(soup)

            if not h2_elements:
                logger.warning(f"⚠️  No valid apartment h2 elements found on page {page}, stopping pagination")
                break

            logger.info(f"🔄 Processing {len(h2_elements)} apartment elements from page {page}")

            parsed_count = 0
            page_had_valid_apartments = False

            for idx, h2_elem in enumerate(h2_elements, 1):
                apt = self.parse_apartment(h2_elem)

                if apt and apt['price'] and apt['link']:
                    all_apartments.append(apt)
                    parsed_count += 1
                    page_had_valid_apartments = True

            logger.info(f"✅ Page {page} complete: Parsed {parsed_count} valid apartments")

            if not page_had_valid_apartments:
                logger.info(f"🛑 No valid apartments found on page {page}, stopping pagination")
                break

            page += 1

        logger.info("=" * 60)
        logger.info(f"✅ Scraping complete: Total {len(all_apartments)} valid apartments found across {page - 1} pages")
        logger.info("=" * 60)
        return all_apartments

    def send_telegram_message(self, message: str, max_retries: int = 3) -> bool:
        logger.info(f"📤 Sending Telegram message: {message[:100]}...")

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
                logger.info(f"📊 Telegram API response status: {response.status_code}")

                if response.status_code == 200:
                    logger.info("✅ Telegram message sent successfully")
                    return True
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 30))
                    logger.warning(f"⚠️ Telegram rate limited, waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                elif response.status_code >= 500:
                    logger.warning(f"⚠️ Telegram server error, retrying...")
                    time.sleep(5 * (attempt + 1))
                    continue
                else:
                    logger.error(f"❌ Failed to send Telegram message: Status {response.status_code}")
                    return False

            except requests.exceptions.Timeout:
                logger.warning(f"⚠️ Telegram timeout on attempt {attempt + 1}, retrying...")
                time.sleep(5 * (attempt + 1))
                continue
            except requests.exceptions.ConnectionError:
                logger.warning(f"⚠️ Telegram connection error on attempt {attempt + 1}, retrying...")
                time.sleep(10 * (attempt + 1))
                continue
            except Exception as e:
                logger.error(f"❌ Error sending Telegram message: {e}", exc_info=True)
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                return False

        logger.error(f"❌ Failed to send Telegram message after {max_retries} attempts")
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

                    logger.info(f"💰 Price change detected for {apt_id}: ₪{old_price:,} → ₪{current_price:,} ({change_pct:+.1f}%)")

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
                logger.info(f"🆕 New apartment detected: {apt_id} - {apt['title'][:50]}")
                new_apartments_list.append(apt)
                self.update_price_history(apt_id, current_price)

            self.apartments[apt_id] = apt

        removed_apartments = set(self.apartments.keys()) - self.current_check_apartments
        for apt_id in removed_apartments:
            logger.info(f"🗑️  Apartment {apt_id} removed from website")
            if apt_id in self.apartments:
                del self.apartments[apt_id]

        logger.info(f"📊 Changes summary - New: {len(new_apartments_list)}, Price changes: {len(price_changes_list)}, Removed: {len(removed_apartments)}")

        if new_apartments_list or price_changes_list:
            self.send_batch_telegram_messages(new_apartments_list, price_changes_list)

    def send_single_telegram_message(self, message: str):
        try:
            self.send_telegram_message(message)
            time.sleep(0.5)
            return True
        except Exception as e:
            logger.error(f"❌ Error sending telegram message: {e}")
            return False

    def send_batch_telegram_messages(self, new_apartments_list: List[Dict], price_changes_list: List[Dict]):
        logger.info("📬 Preparing batch telegram messages...")
        messages_to_send = []

        if new_apartments_list:
            logger.info(f"🆕 Preparing {len(new_apartments_list)} new apartment messages...")
            for apt in new_apartments_list:
                timestamp = datetime.fromisoformat(apt['last_seen']).strftime('%d/%m/%Y %H:%M')
                info_parts = []

                if apt.get('item_info'):
                    info_parts.append(f"📋 {apt['item_info']}")

                info_line = '\n'.join(info_parts) + '\n' if info_parts else ''

                message = (
                    f"🆕 <b>דירה חדשה!</b>\n"
                    f"{'─' * 30}\n\n"
                    f"<b>📍 {apt['title']}</b>\n\n"
                    f"🏠 <b>כתובת:</b> {apt.get('street_address') or apt.get('location') or 'לא צוין'}\n"
                    f"{info_line}"
                    f"💰 <b>מחיר:</b> ₪{apt['price']:,}\n"
                    f"📅 <b>תאריך:</b> {timestamp}\n\n"
                    f"🔗 <a href='{apt['link']}'>לצפייה בדירה</a>"
                )
                messages_to_send.append(message)

        if price_changes_list:
            logger.info(f"💰 Preparing {len(price_changes_list)} price change messages...")
            for change_info in price_changes_list:
                apt = change_info['apartment']
                old_price = change_info['old_price']
                new_price = change_info['new_price']
                change = change_info['change']
                change_pct = change_info['change_pct']
                old_price_date = datetime.fromisoformat(change_info['old_price_date']).strftime('%d/%m/%Y %H:%M')
                new_price_date = datetime.fromisoformat(change_info['new_price_date']).strftime('%d/%m/%Y %H:%M')

                if change < 0:
                    emoji = "📉"
                    change_text = "ירידת מחיר!"
                else:
                    emoji = "📈"
                    change_text = "עליית מחיר"

                info_line = f"📋 {apt['item_info']}\n" if apt.get('item_info') else ''

                message = (
                    f"{emoji} <b>{change_text}</b>\n"
                    f"{'─' * 30}\n\n"
                    f"<b>📍 {apt['title']}</b>\n\n"
                    f"🏠 <b>כתובת:</b> {apt.get('street_address') or apt.get('location') or 'לא צוין'}\n"
                    f"{info_line}\n"
                    f"💵 <b>מחיר קודם:</b> ₪{old_price:,}\n"
                    f"📅 {old_price_date}\n\n"
                    f"💰 <b>מחיר חדש:</b> ₪{new_price:,}\n"
                    f"📅 {new_price_date}\n\n"
                    f"{'🔽' if change < 0 else '🔼'} <b>שינוי:</b> ₪{abs(change):,} ({change_pct:+.1f}%)\n\n"
                    f"🔗 <a href='{apt['link']}'>לצפייה בדירה</a>"
                )
                messages_to_send.append(message)

        if not messages_to_send:
            logger.info("✅ No messages to send.")
            return

        if self.use_parallel_messages:
            try:
                logger.info(f"📤 Sending {len(messages_to_send)} messages in parallel...")
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = [executor.submit(self.send_single_telegram_message, msg) for msg in messages_to_send]
                    for f in as_completed(futures):
                        try:
                            f.result()
                        except Exception as e:
                            logger.error(f"❌ Error in parallel message sending: {e}")
                logger.info("✅ All messages sent (parallel).")
                return
            except RuntimeError as e:
                if "can't start new thread" in str(e):
                    logger.warning("⚠️  Thread creation failed; switching to single-threaded mode.")
                    self.use_parallel_messages = False
                else:
                    logger.error("❌ Unexpected RuntimeError; switching to single-threaded mode.", exc_info=True)
                    self.use_parallel_messages = False
            except Exception:
                logger.error("❌ Unexpected error starting ThreadPool; switching to single-threaded mode.", exc_info=True)
                self.use_parallel_messages = False

        logger.info(f"📤 Sending {len(messages_to_send)} messages in single-threaded mode...")
        for msg in messages_to_send:
            self.send_single_telegram_message(msg)
        logger.info("✅ All messages sent (single-threaded).")

    def monitor(self, url: str):
        logger.info("=" * 80)
        logger.info("🚀 Starting Yad2 Monitor with Adaptive Delay System")
        logger.info("=" * 80)
        logger.info(f"🌐 URL: {url}")
        logger.info(f"🔄 Current delay multiplier: {self.delay_manager.current_multiplier:.2f}x")
        logger.info("=" * 80)

        # Send startup message with adaptive status
        status_report = self.delay_manager.get_status_report()
        self.send_telegram_message(
            "🤖 <b>Yad2 Monitor Started!</b>\n\n"
            f"🔄 <b>Adaptive System Active</b>\n"
            f"📊 Delay multiplier: {self.delay_manager.current_multiplier:.2f}x\n\n"
            "🔍 <b>Status:</b> Active and monitoring..."
        )

        iteration = 0

        while True:
            try:
                iteration += 1
                logger.info("=" * 80)
                logger.info(f"🔄 ITERATION {iteration} - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
                logger.info(f"📊 Current delay multiplier: {self.delay_manager.current_multiplier:.2f}x")
                logger.info("=" * 80)

                logger.info("🔍 Starting apartment scraping across all pages...")
                apartments = self.scrape_all_pages(url)

                if apartments:
                    logger.info(f"✅ Found {len(apartments)} valid apartments total")

                    logger.info("🔍 Checking for changes...")
                    self.check_for_changes(apartments)

                    logger.info("💾 Saving data...")
                    self.save_data()
                    logger.info("✅ Data saved successfully")
                else:
                    logger.warning("⚠️  No valid apartments found in this check")

                logger.info(f"✅ Iteration {iteration} complete")

                # Send periodic status update every 10 iterations
                if iteration % 10 == 0:
                    status_report = self.delay_manager.get_status_report()
                    self.send_telegram_message(status_report)

                interval = self.get_random_interval()
                next_check_time = datetime.fromtimestamp(datetime.now().timestamp() + interval)
                logger.info(f"⏰ Next check at: {next_check_time.strftime('%d/%m/%Y %H:%M:%S')}")
                logger.info(f"😴 Sleeping for {interval // 60} minutes...")

                # Sleep in chunks to allow for interruption
                sleep_start = time.time()
                while time.time() - sleep_start < interval:
                    remaining = interval - (time.time() - sleep_start)
                    remaining_minutes = int(remaining // 60)
                    if remaining_minutes % 10 == 0 and remaining_minutes > 0:
                        logger.info(f"⏳ Still sleeping... {remaining_minutes} minutes until next check")
                    time.sleep(min(60, remaining))

            except KeyboardInterrupt:
                logger.info("⚠️  Keyboard interrupt received")
                logger.info("🛑 Stopping monitor...")
                self.send_telegram_message(
                    "🛑 <b>Yad2 Monitor Stopped</b>\n\n"
                    "Monitor was manually stopped by user."
                )
                break
            except Exception as e:
                logger.error(f"❌ Error in monitoring loop: {e}", exc_info=True)
                self.delay_manager.log_event("error", {"type": "monitor_loop", "exception": str(e)})
                self.send_telegram_message(
                    f"❌ <b>Error in Monitoring Loop</b>\n\n"
                    f"<code>{str(e)}</code>\n\n"
                    "Will retry in 5 minutes..."
                )
                logger.info("⏳ Waiting 5 minutes before retry...")
                time.sleep(300)


if __name__ == "__main__":
    logger.info("🚀 Starting Yad2 Monitor application with Adaptive Delay System")

    # Load credentials from environment variables
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
    YAD2_URL = os.environ.get("YAD2_URL", "https://www.yad2.co.il/realestate/rent?minRooms=4&minSqarremeter=100&topArea=41&area=12&city=8400")
    MIN_INTERVAL_MINUTES = int(os.environ.get("MIN_INTERVAL_MINUTES", "60"))
    MAX_INTERVAL_MINUTES = int(os.environ.get("MAX_INTERVAL_MINUTES", "90"))

    # Validate required environment variables
    if not TELEGRAM_BOT_TOKEN:
        logger.error("❌ TELEGRAM_BOT_TOKEN environment variable is required")
        exit(1)
    if not TELEGRAM_CHAT_ID:
        logger.error("❌ TELEGRAM_CHAT_ID environment variable is required")
        exit(1)

    logger.info("⚙️  Configuration:")
    logger.info(f"  🔑 Telegram Bot Token: {TELEGRAM_BOT_TOKEN[:20]}...")
    logger.info(f"  💬 Telegram Chat ID: {TELEGRAM_CHAT_ID}")
    logger.info(f"  🌐 Yad2 URL: {YAD2_URL}")
    logger.info(f"  ⏱️  Base Interval: {MIN_INTERVAL_MINUTES}-{MAX_INTERVAL_MINUTES} minutes")

    monitor = Yad2Monitor(
        telegram_bot_token=TELEGRAM_BOT_TOKEN,
        telegram_chat_id=TELEGRAM_CHAT_ID,
        min_interval_minutes=MIN_INTERVAL_MINUTES,
        max_interval_minutes=MAX_INTERVAL_MINUTES
    )

    logger.info("🎬 Starting monitor loop with adaptive delays...")
    monitor.monitor(YAD2_URL)

    logger.info("👋 Monitor has stopped")
