-- db/init.sql
CREATE TABLE IF NOT EXISTS customers (
  id serial PRIMARY KEY,
  name text UNIQUE
);

CREATE TABLE IF NOT EXISTS invoices (
  id serial PRIMARY KEY,
  invoice_no text,
  customer text,
  amount numeric,
  currency text,
  issue_date date,
  due_date date,
  status text,
  notes text
);

-- sample rows
INSERT INTO customers (name) VALUES ('Acme Corp') ON CONFLICT DO NOTHING;
INSERT INTO customers (name) VALUES ('Beta Ltd') ON CONFLICT DO NOTHING;

INSERT INTO invoices (invoice_no, customer, amount, currency, issue_date, due_date, status, notes)
VALUES ('INV-1001', 'Acme Corp', 1234.56, 'INR', '2024-11-01', '2024-11-30', 'open', 'first invoice for Acme') ON CONFLICT DO NOTHING;

INSERT INTO invoices (invoice_no, customer, amount, currency, issue_date, due_date, status, notes)
VALUES ('INV-1002', 'Acme Corp', 500.0, 'INR', '2024-10-01', '2024-10-31', 'paid', 'paid via bank transfer') ON CONFLICT DO NOTHING;

INSERT INTO invoices (invoice_no, customer, amount, currency, issue_date, due_date, status, notes)
VALUES ('INV-2001', 'Beta Ltd', 2500.0, 'INR', '2025-01-05', '2025-02-04', 'open', 'monthly subscription') ON CONFLICT DO NOTHING;