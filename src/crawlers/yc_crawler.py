import requests
from bs4 import BeautifulSoup

from src.logging_config import logger
from src.consts import API_URL


class YCCrawler:
    """
    A simple crawler for Y Combinator company pages.

    Fetches and parses company-specific pages hosted on ycombinator.com,
    allowing extraction of structured metadata like the LinkedIn URL.
    """

    BASE_URL = "https://www.ycombinator.com/companies/"

    def __init__(self, slug: str) -> None:
        """
        Initialize the crawler for a specific company slug.

        Args:
            slug (str): The URL slug for the company, e.g., 'nox-metals'.
        """
        self.slug: str = slug
        self.url: str = f"{self.BASE_URL}{slug}"
        self.html: str | None = None
        self.soup: BeautifulSoup | None = None
        self._load()

    def _load(self) -> None:
        """
        Internal method to fetch and parse the company's web page.

        Sets `self.html` and `self.soup` if successful.
        Logs error on failure.
        """
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
        """
        Extract the LinkedIn company URL from the YC company page.

        Returns:
            str: The LinkedIn URL if found, otherwise an empty string.
        """
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

def test_main():
    all_companies = requests.get(API_URL).json()
    slug = all_companies[50]["slug"]
    crawler = YCCrawler(slug)
    
    print(crawler.get_linkedin_url())
    print(slug)

if __name__ == "__main__":
    test_main()