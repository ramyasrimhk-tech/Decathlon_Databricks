"""
Change ONE value in a Silver CSV (in S3) so you can test the CDC loader.

Usage:
    python3 src/edit_silver_for_cdc_test.py TABLE COLUMN NEW_VALUE [--id KEY]

Examples:
    # change customer_city of the FIRST customer row
    python3 src/edit_silver_for_cdc_test.py customers customer_city Hyderabad

    # change a specific customer
    python3 src/edit_silver_for_cdc_test.py customers customer_city Hyderabad --id C0001

The first column of the table is used as the key column.
Raw files are NOT touched. To undo the test, rerun:
    python3 src/01_extract_transform_normalize.py --restart
"""

import io
import sys

import pandas as pd
import requests

BASE_URL = "https://zucau19vp0.execute-api.us-east-1.amazonaws.com"
PROJECT = "Decathlon"
SILVER_PREFIX = "silver/2025/01/01/"

session = requests.Session()


def main():
    args = [a for a in sys.argv[1:]]
    key_value = None

    if "--id" in args:
        i = args.index("--id")
        key_value = args[i + 1]
        del args[i:i + 2]

    if len(args) != 3:
        print(__doc__)
        sys.exit(1)

    table, column, new_value = args
    path = f"{SILVER_PREFIX}{table}.csv"

    # 1. Download the current Silver CSV.
    r = session.get(
        f"{BASE_URL}/processed-download",
        params={"project": PROJECT, "path": path},
        timeout=30,
    )
    r.raise_for_status()
    url = r.json()["download_url"]
    csv = session.get(url, timeout=60)
    csv.raise_for_status()
    df = pd.read_csv(io.BytesIO(csv.content), dtype=str, keep_default_na=False)

    if column not in df.columns:
        sys.exit(f"Column '{column}' not found. Columns: {list(df.columns)}")

    # 2. Change one value.
    key_column = df.columns[0]

    if key_value is None:
        row_index = df.index[0]
        key_value = df.loc[row_index, key_column]
    else:
        matches = df.index[df[key_column] == key_value]
        if len(matches) == 0:
            sys.exit(f"{key_column} = {key_value} not found in {table}.")
        row_index = matches[0]

    old_value = df.loc[row_index, column]
    df.loc[row_index, column] = new_value

    # 3. Upload it back with the same two-step API used by script 01.
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    data = buffer.getvalue().encode("utf-8")

    up = session.post(
        f"{BASE_URL}/upload-url",
        json={"project": PROJECT, "path": path},
        timeout=30,
    )
    up.raise_for_status()
    put = session.put(up.json()["upload_url"], data=data, timeout=120)
    put.raise_for_status()

    print(f"Changed {table}.{column} where {key_column} = {key_value}")
    print(f"  old: {old_value!r}")
    print(f"  new: {new_value!r}")
    print("Now run: python3 src/03_cdc_load_postgres.py --dry-run")


if __name__ == "__main__":
    main()
