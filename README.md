# Nutmeg capital gains calculator

This repository contains a few scripts to help calculate capital gains for a
Nutmeg investment account.

It assumes you have already downloaded a full account activity export from
Nutmeg, in CSV format.

- `import.py`: processes the CSV file and imports data into a SQLite database
- `reconcile.py`: reconciles sale transactions against purchase transactions on a FIFO cost basis
- `summary.py`: generates textual reports on capital gains for a given calendar year

**N.B.** There are almost certainly bugs in this code. Use at your own risk.

NO WARRANTY OF ANY KIND IS OFFERED BY THE ORIGINAL AUTHOR OF THIS CODE
