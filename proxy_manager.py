"""
Proxy Manager Module for Yad2 Monitor
Handles proxy rotation, health checking, and failover
"""
import requests
import random
import time
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import logging
import json

logger = logging.getLogger(__name__)


class ProxyManager:
    """Manages proxy rotation and health monitoring"""

    def __init__(self, proxy_file: str = "proxies.json"):
        self.proxy_file = proxy_file
        self.proxies: List[Dict] = []
        self.proxy_stats: Dict[str, Dict] = defaultdict(lambda: {
            "success": 0,
            "fail": 0,
            "last_used": None,
            "last_success": None,
            "avg_response_time": 0,
            "consecutive_fails": 0
        })
        self.current_proxy_index = 0
        self.cooldown_proxies: Dict[str, datetime] = {}
        self.load_proxies()

    def load_proxies(self):
        """Load proxies from file or environment"""
        # Try loading from file
        if os.path.exists(self.proxy_file):
            try:
                with open(self.proxy_file, 'r') as f:
                    data = json.load(f)
                    self.proxies = data.get('proxies', [])
                    self.proxy_stats = data.get('stats', {})
                    logger.info(f"Loaded {len(self.proxies)} proxies from {self.proxy_file}")
            except Exception as e:
                logger.error(f"Error loading proxies: {e}")

        # Load from environment variable (format: "ip:port,ip:port:user:pass")
        env_proxies = os.environ.get('PROXY_LIST', '')
        if env_proxies:
            for proxy_str in env_proxies.split(','):
                proxy = self.parse_proxy_string(proxy_str.strip())
                if proxy and proxy not in self.proxies:
                    self.proxies.append(proxy)

        # Load single proxy from env
        single_proxy = os.environ.get('HTTP_PROXY') or os.environ.get('HTTPS_PROXY')
        if single_proxy:
            proxy = self.parse_proxy_string(single_proxy)
            if proxy and proxy not in self.proxies:
                self.proxies.append(proxy)

        logger.info(f"Total proxies available: {len(self.proxies)}")

    def parse_proxy_string(self, proxy_str: str) -> Optional[Dict]:
        """Parse proxy string into dict format"""
        if not proxy_str:
            return None

        try:
            # Remove protocol prefix if present
            proxy_str = proxy_str.replace('http://', '').replace('https://', '')

            # Format: ip:port or ip:port:user:pass or user:pass@ip:port
            if '@' in proxy_str:
                auth, hostport = proxy_str.rsplit('@', 1)
                user, password = auth.split(':', 1)
                host, port = hostport.split(':')
            elif proxy_str.count(':') >= 3:
                parts = proxy_str.split(':')
                host, port, user, password = parts[0], parts[1], parts[2], ':'.join(parts[3:])
            else:
                host, port = proxy_str.split(':')
                user, password = None, None

            return {
                'host': host,
                'port': int(port),
                'user': user,
                'password': password
            }
        except Exception as e:
            logger.warning(f"Failed to parse proxy string: {proxy_str}: {e}")
            return None

    def save_proxies(self):
        """Save proxies and stats to file"""
        try:
            with open(self.proxy_file, 'w') as f:
                json.dump({
                    'proxies': self.proxies,
                    'stats': dict(self.proxy_stats)
                }, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Error saving proxies: {e}")

    def add_proxy(self, host: str, port: int, user: str = None, password: str = None):
        """Add a new proxy"""
        proxy = {
            'host': host,
            'port': port,
            'user': user,
            'password': password
        }
        if proxy not in self.proxies:
            self.proxies.append(proxy)
            self.save_proxies()
            logger.info(f"Added proxy: {host}:{port}")

    def remove_proxy(self, host: str, port: int):
        """Remove a proxy"""
        key = f"{host}:{port}"
        self.proxies = [p for p in self.proxies if f"{p['host']}:{p['port']}" != key]
        if key in self.proxy_stats:
            del self.proxy_stats[key]
        self.save_proxies()
        logger.info(f"Removed proxy: {host}:{port}")

    def get_proxy_url(self, proxy: Dict) -> str:
        """Convert proxy dict to URL format"""
        if proxy.get('user') and proxy.get('password'):
            return f"http://{proxy['user']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
        return f"http://{proxy['host']}:{proxy['port']}"

    def get_proxy_key(self, proxy: Dict) -> str:
        """Get unique key for proxy"""
        return f"{proxy['host']}:{proxy['port']}"

    def get_next_proxy(self) -> Optional[Dict]:
        """Get next available proxy using round-robin with health awareness"""
        if not self.proxies:
            return None

        # Clean up expired cooldowns
        now = datetime.now()
        self.cooldown_proxies = {
            k: v for k, v in self.cooldown_proxies.items()
            if v > now
        }

        # Try to find a healthy proxy
        attempts = 0
        while attempts < len(self.proxies):
            proxy = self.proxies[self.current_proxy_index]
            key = self.get_proxy_key(proxy)

            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)
            attempts += 1

            # Skip if in cooldown
            if key in self.cooldown_proxies:
                continue

            # Skip if too many consecutive fails
            stats = self.proxy_stats.get(key, {})
            if stats.get('consecutive_fails', 0) >= 5:
                # Put in cooldown for 30 minutes
                self.cooldown_proxies[key] = now + timedelta(minutes=30)
                continue

            return proxy

        # All proxies in cooldown, return random one anyway
        return random.choice(self.proxies)

    def get_random_proxy(self) -> Optional[Dict]:
        """Get a random proxy (weighted by success rate)"""
        if not self.proxies:
            return None

        # Clean up cooldowns
        now = datetime.now()
        self.cooldown_proxies = {
            k: v for k, v in self.cooldown_proxies.items()
            if v > now
        }

        # Filter available proxies
        available = [
            p for p in self.proxies
            if self.get_proxy_key(p) not in self.cooldown_proxies
        ]

        if not available:
            available = self.proxies

        # Weight by success rate
        weights = []
        for proxy in available:
            key = self.get_proxy_key(proxy)
            stats = self.proxy_stats.get(key, {})
            total = stats.get('success', 0) + stats.get('fail', 0)
            if total > 0:
                success_rate = stats.get('success', 0) / total
                weights.append(0.1 + success_rate * 0.9)  # Minimum weight 0.1
            else:
                weights.append(0.5)  # Unknown proxy gets medium weight

        return random.choices(available, weights=weights, k=1)[0]

    def get_proxies_dict(self, proxy: Dict) -> Dict:
        """Get requests-compatible proxy dict"""
        url = self.get_proxy_url(proxy)
        return {
            'http': url,
            'https': url
        }

    def report_success(self, proxy: Dict, response_time: float = 0):
        """Report successful request through proxy"""
        key = self.get_proxy_key(proxy)
        stats = self.proxy_stats[key]
        stats['success'] += 1
        stats['last_used'] = datetime.now().isoformat()
        stats['last_success'] = datetime.now().isoformat()
        stats['consecutive_fails'] = 0

        # Update average response time
        total = stats['success'] + stats['fail']
        old_avg = stats.get('avg_response_time', 0)
        stats['avg_response_time'] = (old_avg * (total - 1) + response_time) / total

        # Remove from cooldown if present
        if key in self.cooldown_proxies:
            del self.cooldown_proxies[key]

        self.save_proxies()

    def report_failure(self, proxy: Dict, error_type: str = 'unknown'):
        """Report failed request through proxy"""
        key = self.get_proxy_key(proxy)
        stats = self.proxy_stats[key]
        stats['fail'] += 1
        stats['last_used'] = datetime.now().isoformat()
        stats['consecutive_fails'] = stats.get('consecutive_fails', 0) + 1
        stats['last_error'] = error_type

        # Put in short cooldown after failure
        cooldown_minutes = min(5 * stats['consecutive_fails'], 60)
        self.cooldown_proxies[key] = datetime.now() + timedelta(minutes=cooldown_minutes)

        self.save_proxies()

    def test_proxy(self, proxy: Dict, test_url: str = "https://httpbin.org/ip", timeout: int = 10) -> Tuple[bool, float]:
        """Test if proxy is working"""
        try:
            start = time.time()
            response = requests.get(
                test_url,
                proxies=self.get_proxies_dict(proxy),
                timeout=timeout
            )
            elapsed = time.time() - start

            if response.status_code == 200:
                self.report_success(proxy, elapsed)
                return True, elapsed
            else:
                self.report_failure(proxy, f"status_{response.status_code}")
                return False, elapsed

        except requests.exceptions.Timeout:
            self.report_failure(proxy, 'timeout')
            return False, timeout
        except requests.exceptions.ProxyError as e:
            self.report_failure(proxy, 'proxy_error')
            return False, 0
        except Exception as e:
            self.report_failure(proxy, str(type(e).__name__))
            return False, 0

    def test_all_proxies(self, test_url: str = "https://httpbin.org/ip") -> Dict:
        """Test all proxies and return results"""
        results = {
            'working': [],
            'failed': [],
            'total': len(self.proxies)
        }

        for proxy in self.proxies:
            key = self.get_proxy_key(proxy)
            success, response_time = self.test_proxy(proxy, test_url)

            if success:
                results['working'].append({
                    'proxy': key,
                    'response_time': response_time
                })
                logger.info(f"✅ Proxy {key} working ({response_time:.2f}s)")
            else:
                results['failed'].append({
                    'proxy': key,
                    'error': self.proxy_stats.get(key, {}).get('last_error', 'unknown')
                })
                logger.warning(f"❌ Proxy {key} failed")

        return results

    def get_stats(self) -> Dict:
        """Get overall proxy statistics"""
        if not self.proxies:
            return {'total': 0, 'message': 'No proxies configured'}

        total_success = sum(s.get('success', 0) for s in self.proxy_stats.values())
        total_fail = sum(s.get('fail', 0) for s in self.proxy_stats.values())
        total = total_success + total_fail

        healthy = sum(
            1 for p in self.proxies
            if self.proxy_stats.get(self.get_proxy_key(p), {}).get('consecutive_fails', 0) < 3
        )

        return {
            'total_proxies': len(self.proxies),
            'healthy_proxies': healthy,
            'total_requests': total,
            'success_rate': total_success / total if total > 0 else 0,
            'in_cooldown': len(self.cooldown_proxies)
        }

    def get_best_proxy(self) -> Optional[Dict]:
        """Get the best performing proxy"""
        if not self.proxies:
            return None

        best = None
        best_score = -1

        for proxy in self.proxies:
            key = self.get_proxy_key(proxy)
            stats = self.proxy_stats.get(key, {})

            total = stats.get('success', 0) + stats.get('fail', 0)
            if total == 0:
                continue

            success_rate = stats.get('success', 0) / total
            avg_time = stats.get('avg_response_time', 10)

            # Score: success rate * (1 / response_time)
            score = success_rate / (avg_time + 0.1)

            if score > best_score:
                best_score = score
                best = proxy

        return best


class ProxyRotator:
    """High-level proxy rotation for scraping"""

    def __init__(self, proxy_manager: ProxyManager = None):
        self.manager = proxy_manager or ProxyManager()
        self.current_proxy = None
        self.requests_on_current = 0
        self.max_requests_per_proxy = 10

    def get_session(self) -> requests.Session:
        """Get a requests session with proxy configured"""
        session = requests.Session()

        if self.manager.proxies:
            # Rotate proxy if needed
            if not self.current_proxy or self.requests_on_current >= self.max_requests_per_proxy:
                self.current_proxy = self.manager.get_next_proxy()
                self.requests_on_current = 0

            if self.current_proxy:
                session.proxies = self.manager.get_proxies_dict(self.current_proxy)

        self.requests_on_current += 1
        return session

    def make_request(self, url: str, headers: Dict = None, timeout: int = 30, max_retries: int = 3) -> Optional[requests.Response]:
        """Make a request with automatic proxy rotation on failure"""
        last_error = None

        for attempt in range(max_retries):
            proxy = self.manager.get_next_proxy() if self.manager.proxies else None
            proxies = self.manager.get_proxies_dict(proxy) if proxy else None

            try:
                start = time.time()
                response = requests.get(
                    url,
                    headers=headers,
                    proxies=proxies,
                    timeout=timeout
                )
                elapsed = time.time() - start

                if response.status_code == 200:
                    if proxy:
                        self.manager.report_success(proxy, elapsed)
                    return response

                elif response.status_code in [403, 429]:
                    # Blocked or rate limited
                    if proxy:
                        self.manager.report_failure(proxy, f"blocked_{response.status_code}")
                    continue

                else:
                    if proxy:
                        self.manager.report_failure(proxy, f"status_{response.status_code}")
                    continue

            except requests.exceptions.Timeout:
                if proxy:
                    self.manager.report_failure(proxy, 'timeout')
                last_error = "timeout"
            except requests.exceptions.ProxyError:
                if proxy:
                    self.manager.report_failure(proxy, 'proxy_error')
                last_error = "proxy_error"
            except Exception as e:
                if proxy:
                    self.manager.report_failure(proxy, str(type(e).__name__))
                last_error = str(e)

            # Wait before retry
            time.sleep(2 ** attempt)

        logger.error(f"All {max_retries} attempts failed for {url}. Last error: {last_error}")
        return None
