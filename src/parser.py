import requests
import pandas as pd
import polars as pl
from pathlib import Path
from datetime import datetime
import json
from multiprocessing import Pool, cpu_count

from src.logging_config import logger


class Parser:

    def __init__(self, save_dir: str, api_url: str):
        self.save_dir = save_dir
        self.index_path = save_dir / "index.parquet"
        self.api_url = api_url

        self.save_dir.mkdir(parents=True, exist_ok=True)

        self.actual_fields = {
            "slug" : pl.Utf8,
            "name" : pl.Utf8,
            "website": pl.Utf8, 
            "long_description" : pl.Utf8, 
            "yc_url" : pl.Utf8,  
            "linkedin_url" : pl.Utf8, 
            "s25_tag": pl.Boolean,
        }

        if self.index_path.exists():
            self.existing_index = pl.read_parquet(self.index_path)  
        else:
            self.existing_index = pl.DataFrame([
                pl.Series(name=col_name, values=[], dtype=col_type)
                for col_name, col_type in self.actual_fields.items()
            ])

        self.existing_count = len(self.existing_index)

        self.all_companies = requests.get(self.api_url).json()

    def _incomplet_rows(self):
        incomplete_rows = []

        for company in self.all_companies:

            slug = company["slug"]

            row = self.existing_index.select(
                self.actual_fields.keys()
            ).filter(
                pl.col("slug") == slug
            )

            if row.is_empty():
                incomplete_rows.append(company)
                continue

            checks = []

            for col, dtype in self.actual_fields.items():
                if dtype == pl.Utf8:
                    checks.append(
                        (pl.col(col).is_not_null()) & (pl.col(col).str.strip_chars().str.len_bytes() > 0)
                    )
                else:
                    checks.append(pl.col(col).is_not_null())

            # Check if there is at least one missing value in a row
            if not row.select(checks).row(0).count(True) == len(self.actual_fields):
                incomplete_rows.append(company)

        return incomplete_rows
    
    def run(self):
        incomplete_rows = self._incomplet_rows()

        tasks = [(company, self.actual_fields) for company in incomplete_rows]
        
        with Pool(processes=cpu_count() - 1) as pool:
            new_records = pool.map(Parser.process_company, tasks)

        new_rows = pl.DataFrame([r for r in new_records if r is not None])
        new_slugs = new_rows["slug"]

        existing_filtered = self.existing_index.filter(
            ~pl.col("slug").is_in(new_slugs)
        )

        self.existing_index = existing_filtered.vstack(new_rows)
        return self.existing_index
    
    @staticmethod
    def process_company(args):
        company, actual_fields, = args

        slug = company["slug"]
        #crawl here
        record = {
            "slug": slug,
            "name": company["name"],
            "website": company.get("website", ""),
            "long_description": company.get("long_description", ""),
            "yc_url": company["url"],
            "linkedin_url": company.get("linkedin", ""),
            "s25_tag" : False
        }
        return record

    def update(self):
        updates_count = len(self.existing_index) - self.existing_count

        if updates_count > 0:
            self.existing_index.write_parquet(self.index_path)
            logger.info(f"{updates_count} updated in total")
        else: 
            logger.info(f"No updates")