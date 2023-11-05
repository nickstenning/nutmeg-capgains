from __future__ import absolute_import

import argparse
import sqlite3

from decimal import *

parser = argparse.ArgumentParser(description="Print capital gains summary")
parser.add_argument("database", help="Path to database")
parser.add_argument("year", help="Year for which to calculate capital gains")

D = Decimal
QTY_FACTOR = 10000


def iter_results(cur):
    while True:
        batch = cur.fetchmany()
        if not batch:
            break
        yield from batch


def find_year_dividends(cur, year):
    cur.execute(
        """
        select 
            date, 
            assetcode, 
            total,
            (select rate from fx where date <= activities.date order by date desc limit 1) as gbpeur_rate
        from activities
        where description = 'Dividend'
        and date > date(? || '-01-01')
        and date <= date(? || '-01-01', '+1 year')
        order by date asc
    """,
        (str(year), str(year)),
    )
    return iter_results(cur)


def find_year_sales(cur, year):
    cur.execute(
        """
        select
            sales.id as sale_id,
            sales.date as sale_date,
            sales.assetcode as sale_assetcode,
            sales.quantity as sale_quantity,
            sales.price as sale_price,
            sales.total as sale_total,
            (select rate from fx where date <= sales.date order by date desc limit 1) as sale_gbpeur_rate,
            reconciliation.quantity as reconciliation_quantity,
            purchases.id as purchase_id,
            purchases.date as purchase_date,
            purchases.assetcode as purchase_assetcode,
            purchases.quantity as purchase_quantity,
            purchases.price as purchase_price,
            purchases.total as purchase_total
        from activities as sales
        inner join reconciliation on sales.id = reconciliation.sale_id
        inner join activities as purchases on reconciliation.purchase_id = purchases.id
        where sales.description = 'Sale'
        and strftime('%Y', sales.date) = ?
        order by sales.date asc
    """,
        (str(year),),
    )
    return iter_results(cur)


def qty(quantity):
    return D(quantity) / QTY_FACTOR


def single_sale_summary(sale_total, purchase_total, exchange_rate):
    profit = sale_total - purchase_total
    print()
    print(f"    Sale amount            {sale_total:12.4f}")
    print(f"    Cost basis           - {purchase_total:12.4f}")
    print(f"    Profit               = {profit:12.4f}")
    print(f"    GBP/EUR              / {exchange_rate:12.4f}")
    print(f"    Profit (EUR)         = {profit / exchange_rate:12.4f}")


def dividends_summary(conn, year):
    cur = conn.cursor()

    year_dividends = D(0)
    year_dividends_eur = D(0)

    dividends = list(find_year_dividends(cur, year))

    print(f"Date        Ticker  Amount/GBP  GBP/EUR  Amount/EUR")
    for d in dividends:
        gbpeur_rate = D(d["gbpeur_rate"])
        total = D(d["total"])
        total_eur = total / gbpeur_rate
        print(
            f"{d['date']}  {d['assetcode']:6}  {total:10.2f}  {gbpeur_rate:7.4f}  {total_eur:10.2f}"
        )
        year_dividends += total
        year_dividends_eur += total_eur

    return year_dividends, year_dividends_eur


def capital_gains_summary(conn, year):
    cur = conn.cursor()

    year_profit = D(0)
    year_profit_eur = D(0)
    purchase_total = None
    current_sale = None
    current_sale_exchange_rate = None
    current_sale_total = None

    sales = list(find_year_sales(cur, year))

    if not sales:
        return year_profit

    for e in sales:
        if e["sale_id"] != current_sale:
            if current_sale is not None:
                single_sale_summary(
                    current_sale_total, purchase_total, current_sale_exchange_rate
                )
                profit = current_sale_total - purchase_total
                profit_eur = profit / current_sale_exchange_rate
                year_profit += profit
                year_profit_eur += profit_eur

            purchase_total = D(0)
            current_sale = e["sale_id"]
            current_sale_exchange_rate = D(e["sale_gbpeur_rate"])
            current_sale_total = D(e["sale_total"])

            print()
            print(f"Sale of {e['sale_assetcode']}")
            print(
                f"    Sell ({e['sale_date']}): {qty(e['sale_quantity']):19.4f} @ {D(e['sale_price']):9.4f} = {D(e['sale_total']):12.4f}"
            )

        reconciled_proportion = D(e["reconciliation_quantity"]) / D(
            e["purchase_quantity"]
        )
        reconciled_total = reconciled_proportion * D(e["purchase_total"])
        purchase_total += reconciled_total
        print(
            f"    Buy  ({e['purchase_date']}): {qty(e['reconciliation_quantity']):9.4f}/{qty(e['purchase_quantity']):9.4f} @ {D(e['purchase_price']):9.4f} = {reconciled_total:12.4f}"
        )

    single_sale_summary(current_sale_total, purchase_total, current_sale_exchange_rate)
    profit = current_sale_total - purchase_total
    profit_eur = profit / current_sale_exchange_rate
    year_profit += profit
    year_profit_eur += profit_eur

    return year_profit, year_profit_eur


def main():
    args = parser.parse_args()

    conn = sqlite3.connect(args.database)
    conn.row_factory = sqlite3.Row

    cur = conn.cursor()
    cur.execute(
        'select min(strftime("%Y", date)) as min, max(strftime("%Y", date)) as max from activities'
    )
    start, end = cur.fetchone()

    getcontext().traps[FloatOperation] = True

    print(f"CAPITAL INVESTMENTS TAX SUMMARY, {args.year}")
    print("#####################################")
    print()
    print("All amounts are denominated in EUR.")
    print()

    print("Dividends")
    print("---------")
    print()
    dividends, dividends_eur = dividends_summary(conn, args.year)

    print()
    print("Capital gains")
    print("-------------")
    capital_gains, capital_gains_eur = capital_gains_summary(conn, args.year)

    print()
    print(f"Total dividends            {dividends:12.4f}")
    print(f"Total capital gains        {capital_gains:12.4f}")
    print()
    print(f"Total dividends (EUR)      {dividends_eur:12.4f}")
    print(f"Total capital gains (EUR)  {capital_gains_eur:12.4f}")


if __name__ == "__main__":
    main()
