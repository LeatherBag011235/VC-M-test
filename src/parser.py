import requests
import pandas as pd
import polars as pl
from pathlib import Path
from datetime import datetime
import json
from multiprocessing import Pool, cpu_count

from src.crawlers.yc_crawler import YCCrawler
from src.logging_config import logger


class Parser:
    """
    Parses company data from Y Combinator API and enriches it with crawled info.

    Manages a local index of parsed company data, checks for missing fields,
    crawls incomplete entries in parallel, and writes updates to disk.
    """

    def __init__(self, save_dir: Path, index_path: Path, api_url: str) -> None:
        """
        Initialize the parser.

        Args:
            save_dir (Path): Directory to save output files.
            index_path (Path): Path to the local index.parquet file.
            api_url (str): URL to the YC batch JSON API.
        """
        self.save_dir = save_dir
        self.index_path = index_path
        self.api_url = api_url

        self.save_dir.mkdir(parents=True, exist_ok=True)

        self.actual_fields: dict[str, pl.DataType] = {
            "slug": pl.Utf8,
            "name": pl.Utf8,
            "website": pl.Utf8,
            "long_description": pl.Utf8,
            "yc_url": pl.Utf8,
            "linkedin_url": pl.Utf8,
            "s25_tag": pl.Boolean,
        }

        if self.index_path.exists():
            self.existing_index = pl.read_parquet(self.index_path)
        else:
            self.existing_index = pl.DataFrame([
                pl.Series(name=col, values=[], dtype=dt)
                for col, dt in self.actual_fields.items()
            ])

        self.existing_count = len(self.existing_index)
        self.all_companies: list[dict] = requests.get(self.api_url).json()
        self.incomplet_rows = self._incomplet_rows()

    def _incomplet_rows(self) -> list[dict]:
        """
        Identify companies with incomplete data in the existing index.

        Returns:
            list[dict]: Companies needing updates (missing or null fields).
        """
        incomplete_rows = []

        for company in self.all_companies:
            slug = company["slug"]

            row = self.existing_index.select(self.actual_fields.keys()).filter(
                pl.col("slug") == slug
            )

            if row.is_empty():
                incomplete_rows.append(company)
                continue

            if row.null_count().sum_horizontal().item() > 0:
                incomplete_rows.append(company)

        return incomplete_rows

    def run(self) -> pl.DataFrame:
        """
        Parse and crawl missing data using multiprocessing.

        Returns:
            pl.DataFrame: Updated dataframe with new and existing records.
        """
        tasks = [(company, self.actual_fields) for company in self.incomplet_rows]

        with Pool(processes=min(cpu_count() - 1, 20)) as pool:
            new_records = pool.map(Parser.process_company, tasks)

        new_rows = pl.DataFrame([r for r in new_records if r is not None])
        new_slugs = new_rows["slug"]

        existing_filtered = self.existing_index.filter(
            ~pl.col("slug").is_in(new_slugs)
        )

        self.existing_index = existing_filtered.vstack(new_rows)
        return self.existing_index

    @staticmethod
    def process_company(args: tuple[dict, dict[str, pl.DataType]]) -> dict | None:
        """
        Enrich a single company by crawling missing fields.

        Args:
            args (tuple): (company_dict, field_types)

        Returns:
            dict | None: Completed record or None if failed
        """
        company, _ = args
        slug = company["slug"]

        linkedin_url = YCCrawler(slug).get_linkedin_url()

        return {
            "slug": slug,
            "name": company["name"],
            "website": company.get("website"),
            "long_description": company.get("long_description"),
            "yc_url": company["url"],
            "linkedin_url": linkedin_url if linkedin_url else None,
            "s25_tag": False
        }

    def update(self) -> None:
        """
        Log and write updated index to disk.
        """
        additions_count = len(self.existing_index) - self.existing_count
        processed_count = len(self.incomplet_rows)

        logger.info(
            f"{additions_count} companies added; "
            f"{processed_count} companies processed"
        )

        if additions_count + processed_count > 0:
            self.existing_index.write_parquet(self.index_path)


