from pathlib import Path

from src.parser import Parser


SAVE_DIR = Path.home() / ".yc_s25_data"
INDEX_PATH = SAVE_DIR / "index.parquet"
API_URL = "https://yc-oss.github.io/api/batches/summer-2025.json"

def main():
    parser = Parser(save_dir=SAVE_DIR, api_url=API_URL)

    parser.run()
    #parser.update()

if __name__ == "__main__":
    main()