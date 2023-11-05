from __future__ import absolute_import

import argparse
import csv
import datetime
import decimal
import sqlite3
from collections import namedtuple

parser = argparse.ArgumentParser(description="Import Nutmeg data into a database")
parser.add_argument(
    "activities",
    type=argparse.FileType("r", encoding="UTF-8"),
    help="Path to CSV file with trading activity",
)
parser.add_argument("database", help="Path to database")

Activity = namedtuple(
    "Activity",
    [
        "date",
        "description",
        "investment",
        "assetcode",
        "pot",
        "account",
        "quantity",
        "price",
        "total",
    ],
)

QTY_FACTOR = 10000

SCHEMA = """
    create table if not exists activities (
        id integer primary key asc,
        date text not null,
        description text check(description in ('Purchase', 'Sale', 'Fee', 'Dividend', 'Interest')) not null,
        investment text,
        assetcode text,
        pot text,
        account text,
        quantity integer not null,
        price text not null,
        total text not null
    );

    create table if not exists reconciliation (
        id integer primary key asc,
        purchase_id integer not null,
        sale_id integer not null,
        quantity integer not null,
        foreign key(purchase_id) references activity(id),
        foreign key(sale_id) references activity(id)
    );
"""


def get_activities(fp):
    reader = csv.reader(fp)
    # skip header row
    next(reader)

    for line in reader:
        (
            date,
            description,
            investment,
            assetcode,
            pot,
            account,
            quantity,
            price,
            total,
        ) = line
        a = Activity(
            datetime.datetime.strptime(date, "%d-%b-%y").strftime("%Y-%m-%d"),
            description,
            investment,
            assetcode,
            pot,
            account,
            int(decimal.Decimal(quantity) * QTY_FACTOR),
            price,
            total,
        )
        yield a


def main():
    args = parser.parse_args()

    conn = sqlite3.connect(args.database)
    cur = conn.cursor()

    cur.executescript(SCHEMA)
    cur.executemany(
        "insert into activities(date, description, investment, assetcode, pot, account, quantity, price, total) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        get_activities(args.activities),
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
