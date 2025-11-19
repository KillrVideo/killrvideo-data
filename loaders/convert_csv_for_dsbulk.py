#!/usr/bin/env python3
"""
Convert CSV files from CQL format to DSBulk-compatible format.

Key conversions:
- Sets: {"item1","item2"} -> ["item1","item2"]
- Vectors: Already in array format, ensure properly formatted
"""

import csv
import re
import sys
from pathlib import Path


def convert_cql_set_to_json_array(value):
    """
    Convert CQL set format to JSON array format.
    Input:  {"item1","item2","item3"}
    Output: ["item1","item2","item3"]
    """
    if not value or value == "":
        return value

    # CQL sets use curly braces and quoted strings
    # Convert to JSON arrays with square brackets
    if value.startswith("{") and value.endswith("}"):
        # Replace outer braces
        value = "[" + value[1:-1] + "]"

    return value


def convert_csv_file(input_file, output_file, set_columns=None, vector_columns=None):
    """
    Convert a CSV file to DSBulk-compatible format.

    Args:
        input_file: Path to input CSV
        output_file: Path to output CSV
        set_columns: List of column names that contain sets
        vector_columns: List of column names that contain vectors
    """
    set_columns = set_columns or []
    vector_columns = vector_columns or []

    print(f"Converting {input_file.name}...")
    print(f"  Set columns: {set_columns}")
    print(f"  Vector columns: {vector_columns}")

    rows_processed = 0

    with (
        open(input_file, "r", encoding="utf-8") as infile,
        open(output_file, "w", encoding="utf-8", newline="") as outfile,
    ):
        reader = csv.DictReader(infile)

        # Write header
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            # Remove None keys that sometimes appear
            row = {k: v for k, v in row.items() if k is not None}

            # Convert set columns
            for col in set_columns:
                if col in row and row[col]:
                    row[col] = convert_cql_set_to_json_array(row[col])

            # Vector columns should already be in correct format
            # They're already JSON arrays like [1.2, 3.4, 5.6]
            # No conversion needed

            writer.writerow(row)
            rows_processed += 1

            if rows_processed % 1000 == 0:
                print(f"  Processed {rows_processed} rows...")

    print(f"✓ Converted {rows_processed} rows -> {output_file.name}")
    return rows_processed


def main():
    data_dir = Path(__file__).parent.parent / "data"
    output_dir = data_dir / "dsbulk"
    output_dir.mkdir(exist_ok=True)

    print("=== Converting CSV Files for DSBulk ===")
    print(f"Output directory: {output_dir}")
    print()

    # Convert videos.csv
    videos_input = data_dir / "videos.csv"
    if videos_input.exists():
        videos_output = output_dir / "videos.csv"
        convert_csv_file(
            videos_input,
            videos_output,
            set_columns=["tags"],
            vector_columns=["content_features"],
        )

    # Convert tags.csv or tags_cleaned.csv
    tags_input = data_dir / "tags_cleaned.csv"
    if not tags_input.exists():
        tags_input = data_dir / "tags.csv"

    if tags_input.exists():
        tags_output = output_dir / "tags.csv"
        convert_csv_file(
            tags_input,
            tags_output,
            set_columns=["related_tags"],
            vector_columns=["tag_vector"],
        )

    # Convert user_preferences.csv (has maps and vectors)
    user_prefs_input = data_dir / "user_preferences.csv"
    if user_prefs_input.exists():
        user_prefs_output = output_dir / "user_preferences.csv"
        convert_csv_file(
            user_prefs_input,
            user_prefs_output,
            set_columns=[],  # maps are already JSON format
            vector_columns=["preference_vector"],
        )

    print()
    print("=" * 60)
    print("✓ Conversion complete!")
    print(f"Converted files are in: {output_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
