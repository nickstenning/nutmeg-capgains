from __future__ import absolute_import

import argparse
import csv
import datetime
import decimal
import sqlite3
from collections import namedtuple

parser = argparse.ArgumentParser(description="Import GBP/EUR FX data into a database")
parser.add_argument(
    "fx",
    type=argparse.FileType("r", encoding="UTF-8"),
    help="Path to CSV file with GBP/EUR FX rates",
)
parser.add_argument("database", help="Path to database")

Rate = namedtuple(
    "Rate",
    [
        "date",
        "rate",
    ],
)

SCHEMA = """
    create table if not exists fx (
        date text not null,
        rate text not null
    );
"""


def get_rates(fp):
    reader = csv.reader(fp)
    # skip header row
    next(reader)

    for line in reader:
        try:
            date, _, rate = line
        except ValueError:
            date, _ = line
            print(f"skipping {date}: no rate given")
        else:
            yield Rate(date, rate)


def main():
    args = parser.parse_args()

    conn = sqlite3.connect(args.database)
    cur = conn.cursor()

    cur.executescript(SCHEMA)
    cur.executemany(
        "insert into fx(date, rate) values (?, ?)",
        get_rates(args.fx),
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
