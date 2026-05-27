"""
Generate historical accounting transaction data (Jan 26 - May 26, 2026).
Output: CSV file with ~500-1000 synthetic transactions.
Ensures at least one day has negative profit (expenses > revenue).
This version uses only standard library to avoid dependency issues.
"""

import csv
import uuid
import random
from datetime import datetime, timedelta, timezone

# Configuration
START_DATE = datetime(2026, 1, 26, tzinfo=timezone.utc)
END_DATE = datetime(2026, 5, 26, tzinfo=timezone.utc)
OUTPUT_PATH = "historical_transactions.csv"

TRANSACTION_TYPES = ["SALE", "REFUND", "PAYMENT", "FEE", "TRANSFER"]
ENTRY_TYPES = ["DEBIT", "CREDIT"]
STATUSES = ["PENDING", "POSTED", "REVERSED"]
ACCOUNT_NAMES = [
    "Acme Corp", "TechStart Inc", "Global Solutions", "Blue Mountain Ltd",
    "Phoenix enterprises", "Silver Wave Co", "Golden Gate Corp", "Horizon Tech",
    "Nexus Innovations", "Quantum Systems", "Stellar Industries", "Apex Group"
]

# Revenue amounts: positive, simulating income
REVENUE_AMOUNT_RANGE = (500, 5000)
# Expense amounts: positive, simulating costs
EXPENSE_AMOUNT_RANGE = (100, 3000)


def generate_transactions_for_day(date, min_count=2, max_count=5):
    """Generate 2-5 transactions for a given day."""
    num_transactions = random.randint(min_count, max_count)
    transactions = []

    for _ in range(num_transactions):
        # Alternate between REVENUE and EXPENSE to ensure variety
        is_revenue = random.choice([True, False])

        if is_revenue:
            amount = round(random.uniform(*REVENUE_AMOUNT_RANGE), 2)
            category = "REVENUE"
        else:
            amount = round(random.uniform(*EXPENSE_AMOUNT_RANGE), 2)
            category = "EXPENSE"

        # Generate timestamp within the day
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        tx_timestamp = date.replace(hour=hour, minute=minute, second=second)

        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "amount": f"{amount:.2f}",
            "account_name": random.choice(ACCOUNT_NAMES),
            "transaction_type": random.choice(TRANSACTION_TYPES),
            "transaction_timestamp": tx_timestamp.isoformat(),
            "account_category": category,
            "entry_type": random.choice(ENTRY_TYPES),
            "status": random.choice(STATUSES),
            "ingestion_time": tx_timestamp.isoformat(),
        }
        transactions.append(transaction)

    return transactions


def main():
    print(f"Generating historical transactions from {START_DATE.date()} to {END_DATE.date()}...")

    all_transactions = []
    daily_summaries = []

    # Generate transactions for each day
    current_date = START_DATE
    while current_date <= END_DATE:
        transactions = generate_transactions_for_day(current_date)
        all_transactions.extend(transactions)

        # Calculate daily summary
        revenue_sum = sum(
            float(tx["amount"]) for tx in transactions
            if tx["account_category"] == "REVENUE" and tx["status"] == "POSTED"
        )
        expense_sum = sum(
            float(tx["amount"]) for tx in transactions
            if tx["account_category"] == "EXPENSE" and tx["status"] == "POSTED"
        )
        daily_profit = revenue_sum - expense_sum

        daily_summaries.append({
            "date": current_date.date(),
            "revenue": revenue_sum,
            "expense": expense_sum,
            "profit": daily_profit,
        })

        current_date += timedelta(days=1)

    # Print daily summary
    print("\n" + "=" * 70)
    print(f"{'Date':<12} {'Revenue':>12} {'Expense':>12} {'Profit':>12} {'Profit Status':>15}")
    print("=" * 70)

    has_negative_profit = False
    for summary in daily_summaries:
        status = "NEGATIVE ⚠️" if summary["profit"] < 0 else "POSITIVE ✓"
        if summary["profit"] < 0:
            has_negative_profit = True
        print(
            f"{str(summary['date']):<12} ${summary['revenue']:>11.2f} ${summary['expense']:>11.2f} "
            f"${summary['profit']:>11.2f} {status:>15}"
        )

    print("=" * 70)

    if not has_negative_profit:
        print("\n⚠️  WARNING: No day with negative profit found!")
        print("    Forcing one day to have negative profit...")

        # Force the last day to have negative profit by modifying transactions
        last_day = END_DATE
        last_day_txs = [tx for tx in all_transactions if tx["transaction_timestamp"].startswith(str(last_day.date()))]

        if last_day_txs:
            # Flip the first 2+ REVENUE transactions to EXPENSE with larger amounts
            flipped = 0
            for tx in last_day_txs:
                if flipped >= 2:
                    break
                if tx["account_category"] == "REVENUE":
                    tx["account_category"] = "EXPENSE"
                    tx["amount"] = f"{round(random.uniform(*EXPENSE_AMOUNT_RANGE), 2):.2f}"
                    flipped += 1

            print(f"    Flipped {flipped} REVENUE transactions on {last_day.date()} to EXPENSE.")

    print(f"\nGenerated {len(all_transactions)} total transactions across {len(daily_summaries)} days.")

    # Write to CSV
    print(f"Writing to {OUTPUT_PATH}...")
    fieldnames = [
        "transaction_id", "amount", "account_name", "transaction_type",
        "transaction_timestamp", "account_category", "entry_type", "status", "ingestion_time"
    ]

    with open(OUTPUT_PATH, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_transactions)

    print(f"✓ Successfully generated {OUTPUT_PATH}")
    print(f"  Rows: {len(all_transactions)}")
    print(f"\nTo convert to Parquet, run inside docker-compose:")
    print(f"  docker exec trino trino --execute \"")
    print(f"    COPY (SELECT * FROM iceberg.accounting.transactions_historical)")
    print(f"    TO 's3a://warehouse/pipeline-v2-historical/'")
    print(f"    WITH (format = 'PARQUET')\"")


if __name__ == "__main__":
    main()
