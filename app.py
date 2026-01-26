import requests
from bs4 import BeautifulSoup
import time
import json
import os
from datetime import datetime
import hashlib
import random
from typing import Dict, List, Optional
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
        
        logger.info(f"🔑 Telegram Bot Token: {self.telegram_bot_token[:20]}...")
        logger.info(f"💬 Telegram Chat ID: {self.telegram_chat_id}")
        logger.info(f"⏱️  Random interval range: {min_interval_minutes}-{max_interval_minutes} minutes")
        
        logger.info("🔗 Testing Telegram connection...")
        self.test_telegram_connection()
        
        self.load_data()
        
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        logger.info("✅ Initialization complete")
    
    def get_random_interval(self) -> int:
        """Generate random interval between min and max"""
        interval = random.randint(self.min_interval, self.max_interval)
        logger.info(f"🎲 Generated random interval: {interval // 60} minutes ({interval} seconds)")
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
            
            logger.info("📥 Getting bot updates to find available chats...")
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/getUpdates"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                updates = response.json()
                if updates.get('ok') and updates.get('result'):
                    logger.info(f"📨 Found {len(updates['result'])} updates")
                    for update in updates['result'][-5:]:
                        if 'message' in update:
                            chat = update['message'].get('chat', {})
                            logger.info(f"💬 Available chat - ID: {chat.get('id')}, Type: {chat.get('type')}, Username: {chat.get('username', 'N/A')}")
                else:
                    logger.warning("⚠️  No updates found. Make sure you've sent at least one message to the bot!")
            
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
        """Check if element is inside a yad1-listing-data-content box"""
        parent = element.parent
        while parent:
            if parent.name == 'div' and parent.get('class'):
                classes = parent.get('class')
                if 'yad1-listing-data-content_yad1ListingDataContentBox__nWOxH' in classes:
                    return True
            parent = parent.parent
        return False
    
    def find_apartment_elements(self, soup) -> List:
        """Find all h2 elements with data-nagish='content-section-title' that are NOT inside yad1 listing boxes"""
        all_h2_elements = soup.find_all('h2', attrs={'data-nagish': 'content-section-title'})
        logger.info(f"🔍 Found {len(all_h2_elements)} total h2 elements")
        
        valid_elements = [h2 for h2 in all_h2_elements if not self.is_inside_yad1_listing(h2)]
        logger.info(f"✅ Found {len(valid_elements)} valid apartment h2 elements (excluding yad1 listings)")
        return valid_elements
    
    def get_apartment_container(self, h2_element):
        """Get the parent container that holds all apartment information"""
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
        """Extract apartment ID from href or generate hash"""
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
        """Check if link is a valid apartment link"""
        if not link:
            return False
        return "/realestate/item/" in link
    
    def parse_apartment(self, h2_element) -> Optional[Dict]:
        """Parse apartment details from h2 element"""
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
                delay = random.uniform(2, 5) * (attempt + 1)  # Exponential backoff
                logger.info(f"⏳ Waiting {delay:.2f} seconds before fetching page {page} (attempt {attempt + 1}/{max_retries})...")
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
                    wait_time = 300 * (attempt + 1)  # Exponential backoff for rate limit
                    logger.warning(f"⚠️  Rate limited! Waiting {wait_time // 60} minutes...")
                    self.send_telegram_message(f"⚠️ <b>Rate Limited</b>\n\nPausing for {wait_time // 60} minutes...")
                    time.sleep(wait_time)
                    continue

                if response.status_code == 200:
                    logger.info(f"✅ Successfully fetched page {page}, content length: {len(response.text)} bytes")

                    soup = BeautifulSoup(response.text, 'html.parser')
                    block_header = soup.find('h1', class_='title')
                    if block_header and "?Are you for real" in block_header.get_text():
                        logger.warning("🚫 Yad2 suspects scraping! 'Are you for real' page detected.")
                        delay_seconds = random.randint(60, 180) * (attempt + 1)
                        self.send_telegram_message(f"🚫 <b>Scraping Detection</b>\n\nYad2 suspects bot activity.\nPausing for {delay_seconds // 60} minutes...")
                        logger.info(f"⏳ Delaying for {delay_seconds} seconds due to scraping detection.")
                        time.sleep(delay_seconds)
                        continue

                    return response.text
                elif response.status_code >= 500:
                    logger.warning(f"⚠️ Server error {response.status_code}, retrying...")
                    continue
                else:
                    logger.error(f"❌ Error fetching page {page}: Status {response.status_code}")
                    return None

            except requests.exceptions.Timeout:
                logger.warning(f"⚠️ Timeout on attempt {attempt + 1}, retrying...")
                continue
            except requests.exceptions.ConnectionError:
                logger.warning(f"⚠️ Connection error on attempt {attempt + 1}, retrying...")
                time.sleep(30 * (attempt + 1))
                continue
            except Exception as e:
                logger.error(f"❌ Error fetching page {page}: {e}", exc_info=True)
                if attempt < max_retries - 1:
                    continue
                return None

        logger.error(f"❌ Failed to fetch page {page} after {max_retries} attempts")
        return None
    
    def scrape_all_pages(self, base_url: str, max_pages: int = 50) -> List[Dict]:
        logger.info(f"🔍 Starting to scrape pages from {base_url}")
        logger.info(f"📄 Will continue scraping until no apartments found (max {max_pages} pages)")
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
                    # Rate limited - wait and retry
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
        logger.info("🚀 Starting Yad2 Monitor")
        logger.info("=" * 80)
        logger.info(f"🌐 URL: {url}")
        logger.info(f"⏱️  Random interval: {self.min_interval // 60}-{self.max_interval // 60} minutes")
        logger.info("=" * 80)
        
        self.send_telegram_message(
            "🤖 <b>Yad2 Monitor Started!</b>\n\n"
            f"⏱️  <b>Check interval:</b> {self.min_interval // 60}-{self.max_interval // 60} minutes (random)\n"
            "🔍 <b>Status:</b> Active and monitoring..."
        )
        
        iteration = 0
        
        while True:
            try:
                iteration += 1
                logger.info("=" * 80)
                logger.info(f"🔄 ITERATION {iteration} - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
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
                
                interval = self.get_random_interval()
                next_check_time = datetime.fromtimestamp(datetime.now().timestamp() + interval)
                logger.info(f"⏰ Next check at: {next_check_time.strftime('%d/%m/%Y %H:%M:%S')}")
                logger.info(f"😴 Sleeping for {interval // 60} minutes...")
                
                for i in range(interval // 60):
                    remaining_minutes = (interval // 60) - i
                    if remaining_minutes % 5 == 0 and remaining_minutes > 0:
                        logger.info(f"⏳ Still sleeping... {remaining_minutes} minutes until next check")
                    time.sleep(60)
                
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
                self.send_telegram_message(
                    f"❌ <b>Error in Monitoring Loop</b>\n\n"
                    f"<code>{str(e)}</code>\n\n"
                    "Will retry in 5 minutes..."
                )
                logger.info("⏳ Waiting 5 minutes before retry...")
                time.sleep(300)


if __name__ == "__main__":
    logger.info("🚀 Starting Yad2 Monitor application")

    # Load credentials from environment variables
    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
    YAD2_URL = os.environ.get("YAD2_URL", "https://www.yad2.co.il/realestate/rent?minRooms=4&minSqarremeter=100&topArea=41&area=12&city=8400")
    MIN_INTERVAL_MINUTES = int(os.environ.get("MIN_INTERVAL_MINUTES", "80"))
    MAX_INTERVAL_MINUTES = int(os.environ.get("MAX_INTERVAL_MINUTES", "85"))

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
    logger.info(f"  ⏱️  Random Interval: {MIN_INTERVAL_MINUTES}-{MAX_INTERVAL_MINUTES} minutes")

    monitor = Yad2Monitor(
        telegram_bot_token=TELEGRAM_BOT_TOKEN,
        telegram_chat_id=TELEGRAM_CHAT_ID,
        min_interval_minutes=MIN_INTERVAL_MINUTES,
        max_interval_minutes=MAX_INTERVAL_MINUTES
    )

    logger.info("🎬 Starting monitor loop...")
    monitor.monitor(YAD2_URL)

    logger.info("👋 Monitor has stopped")
