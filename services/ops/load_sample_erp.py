# services/ops/load_sample_erp.py
import os
import csv
import psycopg2
from psycopg2.extras import execute_batch

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://cinntra:cinntra@localhost:5432/cinntra")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS customers (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE,
  org_id TEXT
);

CREATE TABLE IF NOT EXISTS invoices (
  id SERIAL PRIMARY KEY,
  invoice_no TEXT,
  customer TEXT,
  amount NUMERIC,
  currency TEXT,
  issue_date DATE,
  due_date DATE,
  status TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS payments (
  id SERIAL PRIMARY KEY,
  invoice_no TEXT,
  paid_amount NUMERIC,
  paid_on DATE,
  payment_method TEXT
);
"""

SAMPLE_CUSTOMERS = [
    ("Acme Corp", "ACME-001"),
    ("Beta Ltd", "BETA-002"),
    ("Gamma LLC", "GAMMA-003"),
]

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()

def insert_sample_customers(conn):
    with conn.cursor() as cur:
        execute_batch(cur,
            "INSERT INTO customers (name, org_id) VALUES (%s,%s) ON CONFLICT (name) DO NOTHING",
            SAMPLE_CUSTOMERS
        )
    conn.commit()

def load_csv_invoices(conn, csv_path):
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            rows.append((
                r.get('invoice_no'),
                r.get('customer'),
                r.get('amount') or 0,
                r.get('currency') or 'INR',
                r.get('issue_date') or None,
                r.get('due_date') or None,
                r.get('status') or 'open',
                r.get('notes') or ''
            ))
    with conn.cursor() as cur:
        execute_batch(cur,
            "INSERT INTO invoices (invoice_no, customer, amount, currency, issue_date, due_date, status, notes) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            rows
        )
    conn.commit()

def load_sample_payments(conn):
    sample = [
        ("INV-1002", 500.00, "2024-12-01", "bank_transfer"),
    ]
    with conn.cursor() as cur:
        execute_batch(cur,
            "INSERT INTO payments (invoice_no, paid_amount, paid_on, payment_method) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            sample
        )
    conn.commit()

def main():
    print("Using DATABASE_URL:", DATABASE_URL)
    conn = psycopg2.connect(DATABASE_URL)
    create_tables(conn)
    insert_sample_customers(conn)
    # load invoices csv if exists
    csv_path = os.path.join(os.path.dirname(__file__), "data", "invoices.csv")
    if os.path.exists(csv_path):
        print("Loading invoices from", csv_path)
        load_csv_invoices(conn, csv_path)
    else:
        print("No invoices.csv found at", csv_path, "- skipping CSV load")
    load_sample_payments(conn)
    conn.close()
    print("ERP sample data loaded.")

if __name__ == "__main__":
    main()
