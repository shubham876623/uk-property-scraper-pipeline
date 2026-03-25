
# epc_async_scraper/proxy_handler.py
import asyncio

class ProxyManager:
    def __init__(self, proxy_file):
        self.proxies = []
        if proxy_file:
            with open(proxy_file, 'r') as f:
                self.proxies = [line.strip() for line in f if line.strip()]
        self.index = 0
        self.lock = asyncio.Lock()  # Async lock for thread-safe access

    async def get_next_proxy(self):
        """Get next proxy in rotation (async thread-safe)"""
        async with self.lock:
            if not self.proxies:
                return None
            proxy = self.proxies[self.index % len(self.proxies)]
            self.index += 1
            return proxy