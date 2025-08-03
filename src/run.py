from pathlib import Path

from src.parser import Parser
from src.consts import SAVE_DIR, INDEX_PATH, API_URL


def main():
    parser = Parser(
        save_dir=SAVE_DIR, 
        index_path=INDEX_PATH, 
        api_url=API_URL,
    )

    parser.run()
    parser.update()

if __name__ == "__main__":
    main()