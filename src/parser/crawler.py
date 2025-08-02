import requests

class Crawler:

    def __init__(self, slug: str):
        self.slug = slug
        

def main():
    API_URL = "https://yc-oss.github.io/api/batches/summer-2025.json"
    all_companies = requests.get(API_URL).json()
    crawler = Crawler(all_companies[0]["slug"])


if __name__ == "__main__":
    main()
