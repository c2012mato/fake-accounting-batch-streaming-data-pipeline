"""
Generate historical accounting transaction data (Jan 1 - May 26, 2026).
Output: Parquet file with ~3500-7000 synthetic transactions (20-50 per day).
Ensures at least one day has negative profit (expenses > revenue).

Requires: pip install pandas pyarrow faker
"""

import sys
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import random

import pandas as pd
from faker import Faker

# Configuration
START_DATE = datetime(2026, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime(2026, 5, 26, tzinfo=timezone.utc)
MIN_TX_PER_DAY = 20
MAX_TX_PER_DAY = 50
OUTPUT_PATH = "historical_transactions.parquet"

fake = Faker()

TRANSACTION_TYPES = ["SALE", "REFUND", "PAYMENT", "FEE", "TRANSFER"]
ENTRY_TYPES = ["DEBIT", "CREDIT"]
STATUSES_WEIGHTED = (
    ["POSTED"] * 8 + ["PENDING"] * 2 + ["REVERSED"] * 1
)
ACCOUNT_NAMES = [
    "Acme Corp", "TechStart Inc", "Global Solutions", "Blue Mountain Ltd",
    "Phoenix Enterprises", "Silver Wave Co", "Golden Gate Corp", "Horizon Tech",
    "Nexus Innovations", "Quantum Systems", "Stellar Industries", "Apex Group",
    "Summit Partners", "Cascade Holdings", "Northwind Traders", "Contoso Ltd",
    "Redwood Analytics", "Palisade Ventures", "Ironclad Security", "Zenith Labs",
]

REVENUE_AMOUNT_RANGE = (200, 8000)
EXPENSE_AMOUNT_RANGE = (50, 6000)

# Days to force negative profit (offset from START_DATE)
FORCE_NEGATIVE_OFFSETS = [14, 41, 72, 103, 130]


def generate_transactions_for_day(
    date: datetime, force_negative: bool = False
) -> List[Dict]:
    num_transactions = random.randint(MIN_TX_PER_DAY, MAX_TX_PER_DAY)
    transactions = []

    for _ in range(num_transactions):
        if force_negative:
            is_revenue = random.random() < 0.2   # 80% expense
        else:
            is_revenue = random.random() < 0.55  # 55% revenue

        if is_revenue:
            amount = round(random.uniform(*REVENUE_AMOUNT_RANGE), 2)
            category = "REVENUE"
        else:
            amount = round(random.uniform(*EXPENSE_AMOUNT_RANGE), 2)
            category = "EXPENSE"

        hour = random.randint(8, 18)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        tx_timestamp = date.replace(hour=hour, minute=minute, second=second)

        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "amount": amount,
            "account_name": random.choice(ACCOUNT_NAMES),
            "transaction_type": random.choice(TRANSACTION_TYPES),
            "transaction_timestamp": tx_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "account_category": category,
            "entry_type": random.choice(ENTRY_TYPES),
            "status": random.choice(STATUSES_WEIGHTED),
            "ingestion_time": tx_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        transactions.append(transaction)

    return transactions


def main():
    print(f"Generating historical transactions from {START_DATE.date()} to {END_DATE.date()}...")
    print(f"Rate: {MIN_TX_PER_DAY}-{MAX_TX_PER_DAY} transactions per day")

    negative_dates = {START_DATE + timedelta(days=d) for d in FORCE_NEGATIVE_OFFSETS}

    all_transactions = []
    current_date = START_DATE
    day_count = 0

    while current_date <= END_DATE:
        force_neg = current_date in negative_dates
        transactions = generate_transactions_for_day(current_date, force_negative=force_neg)
        all_transactions.extend(transactions)
        day_count += 1
        if day_count % 30 == 0:
            print(f"  ... processed {day_count} days ({len(all_transactions)} transactions)")
        current_date += timedelta(days=1)

    print(f"\nGenerated {len(all_transactions)} total transactions across {day_count} days.")
    print(f"Average: {len(all_transactions) / day_count:.1f} tx/day")

    df = pd.DataFrame(all_transactions)
    df["amount"] = df["amount"].astype("float64")

    print(f"Writing to {OUTPUT_PATH}...")
    df.to_parquet(OUTPUT_PATH, index=False)

    print(f"Successfully generated {OUTPUT_PATH}")
    print(f"  Rows: {len(df)}")
    print(f"  Schema: {list(df.columns)}")


if __name__ == "__main__":
    main()
