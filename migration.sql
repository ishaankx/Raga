-- migration.sql
BEGIN;

-- 1) Add a new integer column
ALTER TABLE invoices ADD COLUMN customer_id INTEGER;

-- 2) Populate customer_id by matching customer names (you must have unique customer names)
UPDATE invoices i
SET customer_id = c.id
FROM customers c
WHERE i.customer = c.name;

-- 3) (Optional) If some rows didn't match, check them:
-- SELECT i.* FROM invoices i WHERE i.customer_id IS NULL;

-- 4) Add foreign key (if you're confident)
ALTER TABLE invoices
    ADD CONSTRAINT invoices_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES customers(id);

-- 5) Drop old text column if desired (MAKE SURE you've validated customer_id)
-- ALTER TABLE invoices DROP COLUMN customer;

COMMIT;