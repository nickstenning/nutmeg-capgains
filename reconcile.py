from __future__ import absolute_import

import argparse
import decimal
import sqlite3

parser = argparse.ArgumentParser(
    description="Reconcile sales against purchases on a FIFO cost basis"
)
parser.add_argument("database", help="Path to database")


def iter_results(cur):
    while True:
        batch = cur.fetchmany()
        if not batch:
            break
        yield from batch


def find_unreconciled_sales(conn):
    cur = conn.cursor()
    cur.execute(
        """
        select 
            activities.id,
            activities.date,
            activities.assetcode,
            activities.quantity,
            sum(reconciliation.quantity) as reconciled_quantity
        from activities
        left outer join reconciliation on activities.id = reconciliation.sale_id
        where activities.description = 'Sale'
        group by activities.id, activities.date, activities.assetcode, activities.quantity
        having reconciled_quantity is null or reconciled_quantity < activities.quantity
        order by date asc
    """
    )
    return iter_results(cur)


def find_unreconciled_purchases(cur, assetcode):
    cur.execute(
        """
        select
            activities.id,
            activities.date,
            activities.quantity,
            sum(reconciliation.quantity) as reconciled_quantity
        from activities
        left outer join reconciliation on activities.id = reconciliation.purchase_id
        where activities.description = 'Purchase'
        and activities.assetcode = ?
        group by activities.id, activities.date, activities.quantity
        having reconciled_quantity is null or reconciled_quantity < activities.quantity
        order by date asc
    """,
        (assetcode,),
    )
    return iter_results(cur)


def reconcile(conn, sale):
    cur = conn.cursor()

    reconciled_qty = sale["reconciled_quantity"] or 0
    unreconciled_qty = sale["quantity"] - reconciled_qty

    purchases = list(find_unreconciled_purchases(cur, sale["assetcode"]))

    for pur in purchases:
        # we should not arrive here with no reconciliation to do
        assert unreconciled_qty > 0

        already_reconciled = pur["reconciled_quantity"] or 0
        available_to_reconcile = min(
            unreconciled_qty, pur["quantity"] - already_reconciled
        )

        print(
            f"reconciling {available_to_reconcile} of sale {sale['id']} ({sale['quantity']}) from {sale['date']} against purchase {pur['id']} ({pur['quantity']}) from {pur['date']}"
        )

        # insert reconciliation record
        cur.execute(
            "insert into reconciliation(sale_id, purchase_id, quantity) values (?, ?, ?)",
            (sale["id"], pur["id"], available_to_reconcile),
        )

        unreconciled_qty -= available_to_reconcile

        if unreconciled_qty == 0:
            break

    assert unreconciled_qty == 0, f"failed to fully reconcile sale {sale['id']}"
    conn.commit()


def main():
    args = parser.parse_args()

    conn = sqlite3.connect(args.database)
    conn.row_factory = sqlite3.Row

    sales = list(find_unreconciled_sales(conn))

    for sale in sales:
        reconcile(conn, sale)


if __name__ == "__main__":
    main()
