import requests
from bs4 import BeautifulSoup

from src.logging_config import logger


class YCCrawler:

    BASE_URL = "https://www.ycombinator.com/companies/"

    def __init__(self, slug: str):
        self.slug = slug
        self.url = f"{self.BASE_URL}{slug}"
        self.html = None
        self.soup = None
        self._load()

    def _load(self):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (compatible; YC-S25-Crawler/0.1)"
            }
            response = requests.get(self.url, headers=headers, timeout=20)
            response.raise_for_status()
            self.html = response.text
            self.soup = BeautifulSoup(self.html, "html.parser")
        except Exception as e:
            logger.error(f"[!] Failed to load {self.url}: {e}")

    def get_linkedin_url(self) -> str:
        if not self.soup:
            logger.warning(f"No soup for {self.url}")
            return ""
        
        tags = self.soup.find_all("a", attrs={"aria-label": "LinkedIn profile"})
        for tag in tags:
            tooltip = tag.get("data-tooltip-id", "")
            
            if tooltip.endswith(self.slug) and tag.has_attr("href"):
                return tag["href"]
                
        logger.warning(f"No href or company tooltip in tag for {self.url}")
        return ""

def main():
    API_URL = "https://yc-oss.github.io/api/batches/summer-2025.json"
    all_companies = requests.get(API_URL).json()
    slug = all_companies[50]["slug"]
    crawler = Crawler(slug)
    
    print(crawler.get_linkedin_url())
    print(slug)

if __name__ == "__main__":
    main()