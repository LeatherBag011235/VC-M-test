import requests
import pandas as pd
import polars as pl
from pathlib import Path
from datetime import datetime
import json

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
            "description_yc" : pl.Utf8, 
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

    @staticmethod
    def is_row_complete(row: pl.DataFrame, column_types: dict) -> bool:
        """
        Returns True if the row has no missing values, where:
        - Nulls and empty/whitespace strings are considered missing
        - False/0 are valid
        """
        if row.is_empty():
            return False

        checks = []

        for col, dtype in column_types.items():
            if dtype == pl.Utf8:
                checks.append(
                    (pl.col(col).is_not_null()) & (pl.col(col).str.strip_chars().str.len_bytes() > 0)
                )
            else:
                checks.append(pl.col(col).is_not_null())

        return row.select(checks).row(0).count(True) == len(column_types)
    
    def run(self):
        for company in self.all_companies:
            slug = company["slug"]
            existing_index_slice = self.existing_index.select(
                self.actual_fields.keys()
                ).filter(
                    pl.col("slug") == slug
                    )

            complete = Parser.is_row_complete(existing_index_slice, self.actual_fields)

            if complete:
                continue 

            # Add crawling logic here

            record = {
                "slug": slug,
                "name": company["name"],
                "website": company.get("website", ""),
                "description_yc": company.get("description", ""),
                "yc_url": company["url"],
                "linkedin_url": company.get("linkedin", ""),
                "s25_tag" : False
            }

            add_df = pl.DataFrame(record)
            self.existing_index = self.existing_index.vstack(add_df)

            logger.debug(f"{record["name"]} is updated")

    def update(self):
        updates_count = len(self.existing_index) - self.existing_count

        if updates_count > 0:
            self.existing_index.write_parquet(self.index_path)
            logger.info(f"{updates_count} updated in total")
        else: 
            logger.info(f"No updates")


