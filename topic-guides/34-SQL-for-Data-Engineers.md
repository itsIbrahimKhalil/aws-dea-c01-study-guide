# 34 · SQL for Data Engineers — the query patterns the exam makes you read

> **Exam map:** D3 · Task 3.2 — D1 · Task 1.4 · **Skills:** 3.2.3, 3.2.6, 1.4.1 (plus SQL optimization and Redshift stored procedures) · **Weight:** 🔥🔥🔥 High · **Read time:** ~24 min

## The idea

SQL is a **conversation with a very literal librarian**. You don't tell the librarian *how* to walk the shelves; you describe *what* you want — "every customer in Lahore who ordered more than twice" — and the librarian plans the route. Describe it sloppily and you get a wrong answer delivered with total confidence: duplicate totals because you joined the wrong way, a missing customer because a NULL slipped into a `NOT IN`, or a slow, expensive scan because you asked in a way that stops the librarian from skipping whole aisles (partitions, sort-key blocks).

The DEA-C01 exam uses SQL in two ways: *"which query returns X"* (read four queries, spot the correct one) and *"which change makes this faster/cheaper"* in **Amazon Redshift** and **Amazon Athena**. Skill 3.2.6 names the concepts explicitly: **aggregation, rolling average, grouping, pivoting**. Syntax trivia isn't tested for its own sake (programming-language syntax is out of scope), but you must recognize correct patterns and the classic traps.

Everything below runs against one tiny schema, so you can "see" each answer.

## The running example

```
customers                     orders                                         order_items
customer_id name  city        order_id customer_id order_date  status   fee  order_id sku qty price
1           Ana   Lahore      101      1           2026-09-01  SHIPPED  5    101      A   2   10
2           Ben   Karachi     102      1           2026-09-03  SHIPPED  5    101      B   1   5
3           Cruz  Lahore      103      2           2026-09-03  CANCELLED 0   102      A   1   10
4           Dev   NULL        104      3           2026-09-05  SHIPPED  5    103      C   3   7
                              105      NULL        2026-09-06  NEW      5    104      B   4   5
                                                                             105      A   1   10
```

Order 105 is a guest checkout (NULL customer). Line revenue = `qty * price`: order 101 = 25, 102 = 10, 103 = 21, 104 = 20, 105 = 10. Total shipping fees = 20.

## Joins, semi-joins, anti-joins

| Join | Returns |
|---|---|
| `INNER JOIN` | Only matching rows on both sides |
| `LEFT [OUTER] JOIN` | All left rows; NULLs where the right has no match |
| `RIGHT` / `FULL [OUTER] JOIN` | All right rows / all rows from both sides |
| `CROSS JOIN` | Every combination (rows × rows) — intended for date spines and UNNEST, dangerous otherwise |
| **Semi-join** `WHERE EXISTS (...)` / `IN (...)` | Left rows that have at least one match — **never duplicates** the left side |
| **Anti-join** `WHERE NOT EXISTS (...)` / `LEFT JOIN ... WHERE right.key IS NULL` | Left rows with **no** match |

*"Customers who have never ordered"* — the right answer is **Dev**:

```sql
SELECT c.name
FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);
-- Dev
```

**THE trap — `NOT IN` with NULLs:**

```sql
SELECT name FROM customers
WHERE customer_id NOT IN (SELECT customer_id FROM orders);
-- returns ZERO rows
```

`orders.customer_id` contains a NULL (order 105). `4 NOT IN (1, 2, 3, NULL)` evaluates to *unknown*, never true, so every row is filtered out. Use `NOT EXISTS`, the `LEFT JOIN ... IS NULL` anti-join, or add `WHERE customer_id IS NOT NULL` in the subquery.

**THE trap — join fan-out:** *"Total shipping fees for shipped orders"* (correct answer: 15).

```sql
-- WRONG: joining items first duplicates order-level columns
SELECT SUM(o.fee)
FROM orders o JOIN order_items i ON i.order_id = o.order_id
WHERE o.status = 'SHIPPED';          -- 20: order 101 has two items, its fee counted twice

-- RIGHT: aggregate at the grain of the column you sum
SELECT SUM(fee) FROM orders WHERE status = 'SHIPPED';   -- 15
```

A one-to-many join repeats the "one" side once per "many" row. Sum order-level measures at the order grain, or pre-aggregate the many side in a CTE before joining.

## Aggregation, WHERE vs HAVING, and grouping sets

```sql
SELECT COUNT(*), COUNT(city), COUNT(DISTINCT city) FROM customers;
-- 4, 3, 2      (* counts rows; COUNT(col) skips NULLs; DISTINCT dedupes non-null values)
```

**THE trap — WHERE vs HAVING.** `WHERE` filters **rows before** grouping; `HAVING` filters **groups after** aggregation. Aggregates can't appear in `WHERE`.

*"Customers whose shipped revenue exceeds 20"* → Ana (35); Cruz has exactly 20.

```sql
SELECT o.customer_id, SUM(i.qty * i.price) AS revenue
FROM orders o JOIN order_items i ON i.order_id = o.order_id
WHERE o.status = 'SHIPPED'                 -- row filter
GROUP BY o.customer_id
HAVING SUM(i.qty * i.price) > 20;          -- group filter
-- customer 1 | 35
```

Putting `status = 'SHIPPED'` in `HAVING` fails (not grouped); putting `SUM(...) > 20` in `WHERE` is a syntax error.

**Subtotals in one pass** — `GROUPING SETS`, `ROLLUP`, `CUBE` are supported in **both Redshift and Athena**:

```sql
SELECT c.city, o.customer_id, SUM(i.qty * i.price) AS revenue
FROM orders o
JOIN customers c   ON c.customer_id = o.customer_id
JOIN order_items i ON i.order_id = o.order_id
WHERE o.status = 'SHIPPED'
GROUP BY ROLLUP (c.city, o.customer_id);
-- Lahore | 1    | 35
-- Lahore | 3    | 20
-- Lahore | NULL | 55    <- city subtotal
-- NULL   | NULL | 55    <- grand total
```

`ROLLUP(a, b)` = sets (a, b), (a), (); `CUBE(a, b)` = every combination including (b) alone; `GROUPING SETS` lists exactly the sets you want. One scan instead of several `UNION ALL`ed queries.

## Window functions

A window function computes across related rows **without collapsing them** (unlike `GROUP BY`): `function() OVER (PARTITION BY ... ORDER BY ... frame)`.

Ranking orders by date (orders 102 and 103 tie on 2026-09-03):

| order_id | order_date | ROW_NUMBER | RANK | DENSE_RANK |
|---|---|---|---|---|
| 101 | 09-01 | 1 | 1 | 1 |
| 102 | 09-03 | 2 | 2 | 2 |
| 103 | 09-03 | 3 | 2 | 2 |
| 104 | 09-05 | 4 | **4** | **3** |
| 105 | 09-06 | 5 | 5 | 4 |

`ROW_NUMBER` = unique sequence (ties broken arbitrarily unless you add a tiebreaker column); `RANK` = ties share a rank and **leave gaps**; `DENSE_RANK` = ties share, **no gaps**; `NTILE(4)` = quartile buckets.

**Latest order per customer** (top-N per group):

```sql
SELECT * FROM (
  SELECT o.*, ROW_NUMBER() OVER (PARTITION BY customer_id
                                 ORDER BY order_date DESC, order_id DESC) AS rn
  FROM orders o
  WHERE customer_id IS NOT NULL
) t
WHERE rn = 1;                          -- 102 (customer 1), 103 (customer 2), 104 (customer 3)
```

Redshift can shorten this with **`QUALIFY rn = 1`** (filter on a window result, like HAVING for windows). **Athena's documented SELECT syntax has no QUALIFY** — use the subquery form, which works everywhere.

**LAG / LEAD / FIRST_VALUE** — compare to neighbors: `revenue - LAG(revenue) OVER (ORDER BY sale_date)` = day-over-day change; `LEAD` looks ahead; `FIRST_VALUE(x) OVER (PARTITION BY ... ORDER BY ...)` = the first value in the window. `LAST_VALUE` is a trap: with the default frame it returns the **current** row — specify `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

**Running total and 7-day rolling average (skill 3.2.6)** on a `daily_sales(sale_date, revenue)` table:

```sql
SELECT sale_date,
       revenue,
       SUM(revenue) OVER (ORDER BY sale_date
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
       AVG(revenue) OVER (ORDER BY sale_date
                          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)         AS avg_7d
FROM daily_sales;
```

`6 PRECEDING AND CURRENT ROW` = 7 rows. Add `PARTITION BY store_id` for a per-store rolling average.

**THE trap — ROWS vs RANGE.** `ROWS` counts **physical rows**; if days are missing, "6 preceding rows" can span 10 calendar days. Fixes: join to a **date spine** (one row per day, revenue 0 for gaps) before windowing, or use a value-based `RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW` frame where the engine supports it (Athena/Trino does). Redshift frames are written with `ROWS`, and Redshift requires an explicit frame clause when an aggregate window function has `ORDER BY` — the pattern above is the portable one. A rolling average is also the textbook case for **window vs self-join**: the self-join version (`JOIN d2 ON d2.sale_date BETWEEN d1.sale_date - 6 AND d1.sale_date`) is slower and harder to read.

## Pivoting and unpivoting

Pivot = turn row values into columns. The **portable** way (works in Redshift, Athena, anywhere) is conditional aggregation:

```sql
SELECT sku_group,
       SUM(CASE WHEN sku = 'A' THEN qty ELSE 0 END) AS qty_a,
       SUM(CASE WHEN sku = 'B' THEN qty ELSE 0 END) AS qty_b,
       SUM(CASE WHEN sku = 'C' THEN qty ELSE 0 END) AS qty_c
FROM (SELECT 'all' AS sku_group, sku, qty FROM order_items) t
GROUP BY sku_group;
-- all | 4 | 5 | 3
```

**Redshift** has native `PIVOT` / `UNPIVOT`:

```sql
SELECT * FROM (SELECT sku, qty FROM order_items)
PIVOT (SUM(qty) FOR sku IN ('A', 'B', 'C'));
-- 4 | 5 | 3
```

**Athena** has no `PIVOT` keyword — use `CASE` aggregation, or `map_agg(sku, qty)` to build a key→value map per group; unpivot with `CROSS JOIN UNNEST` over an array or map. Pivoting needs the output column list known in advance; a dynamic list means dynamic SQL (e.g., a Redshift stored procedure).

## CTEs, recursion, and subqueries

A **CTE** (`WITH name AS (...)`) names an intermediate result — readable, reusable within the query, and the natural place to **pre-aggregate before joining**. **Recursive CTEs** walk hierarchies and trees (org charts, bill of materials, category trees — skill 1.4.11):

```sql
WITH RECURSIVE org (emp_id, manager_id, lvl) AS (
    SELECT emp_id, manager_id, 1 FROM employees WHERE manager_id IS NULL   -- anchor: the root
    UNION ALL
    SELECT e.emp_id, e.manager_id, o.lvl + 1                               -- step: children
    FROM employees e JOIN org o ON e.manager_id = o.emp_id
)
SELECT * FROM org ORDER BY lvl;
```

Supported in **Redshift** (`WITH RECURSIVE`, column list required) and **Athena engine v3** (maximum recursion depth **10**) — for deep graphs, use a graph database (Amazon Neptune) or iterative processing instead.

Subquery vs join: correlated subqueries re-evaluate per row in naive plans; rewrite as joins or window functions for big tables. `EXISTS` stops at the first match — prefer it over `IN` with a large subquery, and over a `JOIN` + `DISTINCT` for existence tests.

## Deduplication, upserts, and SCD Type 2

**Dedup** — keep the latest version of each key from an at-least-once feed:

```sql
SELECT order_id, status, ingested_at FROM (
  SELECT s.*, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingested_at DESC) AS rn
  FROM stg_orders s
) t WHERE rn = 1;
```

`SELECT DISTINCT` only removes rows identical in **every** column — useless when versions differ by timestamp.

**Upsert with MERGE** — **Redshift** (native) and **Athena on Apache Iceberg tables**:

```sql
MERGE INTO orders
USING stg_orders s ON orders.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET status = s.status, fee = s.fee
WHEN NOT MATCHED THEN INSERT VALUES (s.order_id, s.customer_id, s.order_date, s.status, s.fee);
```

Deduplicate the source first — a target row matched by several source rows is an error. The classic Redshift alternative (and what older questions describe) is the **staging-table pattern** inside one transaction:

```sql
BEGIN;
DELETE FROM orders USING stg_orders s WHERE orders.order_id = s.order_id;
INSERT INTO orders SELECT order_id, customer_id, order_date, status, fee FROM stg_orders;
END;
```

On plain Hive-style Athena tables (non-Iceberg) there is no row-level `UPDATE`/`MERGE` — rewrite partitions with CTAS/INSERT, or move to Iceberg ([Guide 04](04-Open-Table-Formats-S3-Tables.md), [Guide 26](26-Amazon-Athena.md)).

**SCD Type 2** (concept in [Guide 30](30-Data-Modeling-Schema-Evolution-Lineage.md)) on `dim_customer(customer_sk IDENTITY, customer_id, name, city, effective_from, effective_to, is_current)` from a staging table `stg_customer(customer_id, name, city, change_date)`:

```sql
BEGIN;
-- 1) Expire the current version when a tracked attribute changed
UPDATE dim_customer
SET effective_to = s.change_date, is_current = FALSE
FROM stg_customer s
WHERE dim_customer.customer_id = s.customer_id
  AND dim_customer.is_current = TRUE
  AND COALESCE(dim_customer.city, '') <> COALESCE(s.city, '');     -- NULL-safe compare

-- 2) Insert a new current version for changed AND brand-new customers
INSERT INTO dim_customer (customer_id, name, city, effective_from, effective_to, is_current)
SELECT s.customer_id, s.name, s.city, s.change_date, DATE '9999-12-31', TRUE
FROM stg_customer s
LEFT JOIN dim_customer d
       ON d.customer_id = s.customer_id AND d.is_current = TRUE
WHERE d.customer_id IS NULL;           -- no current row = new customer or just expired
END;
```

After step 1, changed customers have no current row, so step 2's anti-join picks them up together with new customers; unchanged customers still have a current row and are skipped. The surrogate key comes from the `IDENTITY` column. Forgetting step 1 (or the transaction) leaves two "current" rows per customer — the classic SCD2 bug. A plain `MERGE ... WHEN MATCHED THEN UPDATE` is **Type 1** (overwrite), not Type 2.

## Dates, strings, NULLs — dialect differences

| Task | Redshift | Athena (Trino) |
|---|---|---|
| Truncate to month | `DATE_TRUNC('month', ts)` | `date_trunc('month', ts)` |
| Add 7 days | `DATEADD(day, 7, d)` or `d + INTERVAL '7 days'` | `date_add('day', 7, d)` or `d + INTERVAL '7' DAY` |
| Difference in days | `DATEDIFF(day, a, b)` | `date_diff('day', a, b)` |
| Part of a date | `EXTRACT(year FROM d)`, `DATE_PART` | `EXTRACT(year FROM d)`, `year(d)` |
| Now | `GETDATE()`, `SYSDATE` | `current_timestamp`, `now()` |
| NULL handling | `COALESCE`, `NVL`, `NULLIF` | `COALESCE`, `NULLIF` |
| Safe cast | Validate first (e.g., `CASE WHEN col ~ '^[0-9]+$' THEN CAST(...)`) | **`TRY_CAST(col AS integer)`** returns NULL instead of failing |
| Parse JSON | `JSON_PARSE(str)` → `SUPER`; PartiQL navigation | `json_extract_scalar(str, '$.a.b')`, `json_parse` |
| Upsert | `MERGE` (native tables) | `MERGE INTO` on **Iceberg** only |
| Pivot | `PIVOT` / `UNPIVOT` | `CASE` / `map_agg` |
| Filter on window result | `QUALIFY` | Subquery |
| Approximate distinct | `APPROXIMATE COUNT(DISTINCT x)` | `approx_distinct(x)` |
| Materialized view | `CREATE MATERIALIZED VIEW` (+ `AUTO REFRESH`) | Views only — precompute with CTAS/INSERT INTO |

`NULLIF(qty, 0)` in a denominator prevents divide-by-zero errors; `COALESCE(city, 'Unknown')` for display. Any comparison with NULL (`= NULL`) is unknown — use `IS NULL`.

**THE trap:** a query fails mid-way on one malformed value in a raw CSV table in Athena — the fix is `TRY_CAST` (plus a quality check), not re-running with more capacity.

## Semi-structured SQL

**Redshift `SUPER` + PartiQL** — load JSON into a `SUPER` column (`JSON_PARSE` or COPY), then navigate with dots/brackets and unnest arrays in the `FROM` clause:

```sql
SELECT o.order_id, o.payload.customer.name AS customer, i.sku, i.qty
FROM orders_json o, o.payload.items AS i          -- unnest: one row per array element
WHERE i.qty > 1;
```

(Mixed-case JSON attribute names need `enable_case_sensitive_identifier` turned on.)

**Athena** — arrays of structs expand with `UNNEST`:

```sql
SELECT o.order_id, item.sku, item.qty
FROM orders_nested o
CROSS JOIN UNNEST(o.items) AS t(item);

SELECT json_extract_scalar(raw, '$.customer.name') FROM orders_raw;   -- JSON stored as string
```

## Views and materialized views

- **Views** (`CREATE OR REPLACE VIEW`) in both engines — saved queries, no stored data; Athena stores them in the Glue Data Catalog. Good for simplifying access and hiding columns.
- **Redshift materialized views** store precomputed results; `REFRESH MATERIALIZED VIEW` (incremental where possible) or `AUTO REFRESH YES`; the optimizer can **automatically rewrite** queries to use them. Pick for *"dashboards repeatedly run the same expensive aggregation"*. MVs can also be built over Spectrum, federated, and streaming-ingestion sources ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)).
- **Late-binding views** — `CREATE VIEW ... WITH NO SCHEMA BINDING` — don't lock the underlying tables: you can drop/recreate tables without dropping the view, and they're **required for views over external (Spectrum) tables**.

## Redshift stored procedures

Procedural SQL (PL/pgSQL) stored in the database — the ELT workhorse for multi-step loads:

```sql
CREATE OR REPLACE PROCEDURE etl.load_day(p_day DATE, p_table VARCHAR)
AS $$
DECLARE
    v_rows INT;
BEGIN
    DELETE FROM sales WHERE sale_date = p_day;
    INSERT INTO sales SELECT * FROM stg_sales WHERE sale_date = p_day;
    GET DIAGNOSTICS v_rows := ROW_COUNT;
    RAISE INFO 'Loaded % rows for %', v_rows, p_day;

    EXECUTE 'ANALYZE ' || quote_ident(p_table);            -- dynamic SQL
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'load_day failed: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

CALL etl.load_day('2026-09-26', 'sales');
```

- Parameters (`IN`, `OUT`, `INOUT`), variables, `IF`/`CASE`, loops (`FOR r IN SELECT ... LOOP ... END LOOP`, `WHILE`), **dynamic SQL** with `EXECUTE` (build statements from table names — pivots with dynamic column lists, per-partition maintenance), `RAISE` for messages and errors, an `EXCEPTION` block for handling.
- **Transactions:** by default the procedure body runs inside a transaction; `COMMIT`/`ROLLBACK` inside the body end the current transaction and start a new one, and `TRUNCATE` commits implicitly. An error that isn't handled rolls back uncommitted work. The **`NONATOMIC`** option (declared on `CREATE PROCEDURE`) makes statements auto-commit individually, so earlier steps persist even if a later one fails — you manage transaction boundaries yourself.
- Security: `SECURITY INVOKER` (default) or `SECURITY DEFINER` (run with the owner's privileges — grant EXECUTE instead of table access).
- **Scheduling:** Redshift query editor v2 scheduled queries (EventBridge under the hood), EventBridge Scheduler + the **Redshift Data API**, Step Functions, or MWAA ([Guide 24](24-Redshift-Loading-Integration-Sharing.md)).

> ⚠️ **2026 status:** Redshift **Python UDFs** are no longer supported after **June 30, 2026** (phased enforcement). For custom logic use **SQL UDFs**, **Lambda UDFs**, or stored procedures.

## Query optimization (skill 1.4.1)

| Technique | Why it works |
|---|---|
| **Select only needed columns** (no `SELECT *`) | Column pruning — columnar engines read less; Athena bills bytes scanned |
| **Filter on the partition column itself** | Partition pruning; filtering on `event_ts` when the table is partitioned by `dt` scans everything |
| **Sargable predicates** — compare the raw column to literals | Wrapping a sort key or partition key in a function (`DATE_TRUNC(ts) = ...`, `CAST(dt AS date) = ...`) can stop zone-map block skipping and partition pruning; write ranges instead: `ts >= '2026-09-01' AND ts < '2026-10-01'` |
| **Predicate pushdown** | Filters evaluated at the storage layer (Parquet row-group statistics, Spectrum) |
| **Reduce before join** | Filter/aggregate in CTEs first, join smaller sets; put the **larger table on the left** in Athena joins |
| **Join on the DISTKEY** (Redshift) | Co-located join, no redistribution ([Guide 23](23-Redshift-Architecture-Table-Design.md)) |
| **`EXISTS` over `IN` / `JOIN + DISTINCT`** | Stops at first match; no dedupe step |
| **`UNION ALL` over `UNION`** | `UNION` deduplicates (hash/sort) — skip when duplicates can't exist or don't matter |
| **Window over self-join** | One pass instead of a quadratic join |
| **Approximate aggregates** | `APPROXIMATE COUNT(DISTINCT)` / `approx_distinct` use HyperLogLog: a small error for big speedups on huge cardinalities |
| **Materialize intermediates** | CTAS/temp tables or MVs for results reused many times; `ANALYZE` after big loads |
| **`EXPLAIN`** (both), `EXPLAIN ANALYZE` (Athena), system views (Redshift) | See scans, join order, redistribution (`DS_BCAST_INNER`, `DS_DIST_BOTH`) ([Guide 25](25-Redshift-Performance-Operations-Security.md)) |
| **Top-N with `ORDER BY ... LIMIT`** | Engines optimize top-N; plain `LIMIT` doesn't reduce bytes scanned on large unpartitioned tables |

**THE trap:** *"Athena query cost is high; the table is partitioned by dt"* and the query filters `WHERE date(event_ts) = DATE '2026-09-01'` — add `AND dt = '2026-09-01'` (filter the partition column) and select fewer columns; buying capacity or adding LIMIT doesn't cut bytes scanned.

## Question patterns

> *"Which query lists customers who have never placed an order, given that orders.customer_id can be NULL for guest checkouts?"* → **`SELECT c.name FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)`** (`NOT IN` returns zero rows when the subquery holds a NULL).

> *"A report sums orders.fee after joining orders to order_items and the total is higher than finance's number."* → **Join fan-out — sum the fee at the order grain (`SELECT SUM(fee) FROM orders ...`) or pre-aggregate items in a CTE before joining**.

> *"Which query returns only customers whose total shipped revenue is greater than 1,000?"* → **`... WHERE status = 'SHIPPED' GROUP BY customer_id HAVING SUM(qty * price) > 1000`** (aggregate conditions go in HAVING; row conditions in WHERE).

> *"Return each store's daily revenue with a 7-day rolling average."* → **`AVG(revenue) OVER (PARTITION BY store_id ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`** (on a gap-free date spine; `GROUP BY` would collapse the daily rows).

> *"Return the most recent order for each customer, keeping all order columns."* → **`ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC)` then filter `rn = 1`** (QUALIFY in Redshift; subquery in Athena). `MAX(order_date)` + `GROUP BY` loses the other columns.

> *"Top 3 products per category where tied products must all be included and ranks must not skip numbers."* → **`DENSE_RANK()` ≤ 3** (RANK leaves gaps; ROW_NUMBER drops ties).

> *"Produce revenue per region per month plus region subtotals and a grand total in one query, reading the data once."* → **`GROUP BY ROLLUP (region, month)`** (works in Redshift and Athena; UNION ALL of three queries scans three times).

> *"Show quantity per SKU as columns A, B, C in Athena."* → **`SUM(CASE WHEN sku = 'A' THEN qty END)` per column** (Athena has no PIVOT keyword; Redshift could use PIVOT).

> *"A Kinesis-fed staging table contains several versions of each order; load only the latest version into Redshift, updating existing rows and inserting new ones."* → **Dedupe with ROW_NUMBER, then `MERGE INTO` (or staging DELETE + INSERT in one transaction)**.

> *"Analysts need an upsert on an S3 data lake table queried by Athena, with the least operational overhead."* → **Convert the table to Apache Iceberg and use Athena `MERGE INTO`** (Hive-style tables have no row-level updates).

> *"Implement SCD Type 2 for a customer dimension in Redshift."* → **In one transaction: UPDATE current rows whose attributes changed (set effective_to, is_current = false), then INSERT new current rows for changed and new customers** (a single MERGE UPDATE is Type 1).

> *"Walk a manager → employee hierarchy of arbitrary depth in Redshift SQL."* → **`WITH RECURSIVE` CTE (anchor UNION ALL recursive step)** (Athena caps recursion depth at 10).

> *"Nested JSON order payloads are stored in a Redshift SUPER column; return one row per line item."* → **PartiQL unnesting: `FROM orders_json o, o.payload.items AS i`**.

> *"A dashboard runs the same heavy aggregation on Redshift every few minutes."* → **Materialized view with auto refresh (automatic query rewrite)**.

> *"Count distinct users over 10 billion clickstream rows in Athena; a ~2% error is acceptable and speed matters."* → **`approx_distinct(user_id)`** (Redshift equivalent: `APPROXIMATE COUNT(DISTINCT user_id)`).

> *"Athena query on a table partitioned by dt scans the whole table even though it filters one day of event_ts."* → **Filter on the partition column (`dt = '...'`) and select only needed columns**.

## Pocket card

| Keyword / signal | Answer |
|---|---|
| Rows with no match | `NOT EXISTS` or `LEFT JOIN ... IS NULL` |
| `NOT IN` returns nothing | NULL in the subquery |
| Sum too high after join | Fan-out — aggregate at the right grain |
| Existence test | `EXISTS` (semi-join, no duplicates) |
| Filter groups | `HAVING` (rows: `WHERE`) |
| COUNT(*) vs COUNT(col) | Rows vs non-null values |
| Subtotals + grand total, one scan | `ROLLUP` / `CUBE` / `GROUPING SETS` (both engines) |
| Top-N per group | `ROW_NUMBER() OVER (PARTITION BY ...)` + filter |
| Ties share rank, no gaps | `DENSE_RANK` (gaps: `RANK`) |
| Previous / next row | `LAG` / `LEAD` |
| Running total | `SUM() OVER (ORDER BY ... ROWS UNBOUNDED PRECEDING)` |
| 7-day rolling average | `AVG() OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)` |
| Missing days in rolling window | Date spine (or RANGE interval frame in Athena) |
| Filter on window result | Redshift `QUALIFY`; Athena subquery |
| Rows → columns, portable | `CASE` inside `SUM`/`COUNT` |
| Native pivot | Redshift `PIVOT`/`UNPIVOT`; Athena none (`map_agg`) |
| Hierarchy / tree | `WITH RECURSIVE` (Athena depth ≤ 10) |
| Latest version per key | `ROW_NUMBER() ... ORDER BY ingested_at DESC` = 1 |
| Upsert in Redshift | `MERGE` or staging DELETE + INSERT in a transaction |
| Upsert in Athena | `MERGE INTO` on Iceberg only |
| SCD2 in SQL | Expire changed current rows, then insert new versions |
| Bad values break a cast (Athena) | `TRY_CAST` |
| Add days | Redshift `DATEADD(day, n, d)` / Athena `date_add('day', n, d)` |
| JSON in Redshift | `SUPER` + PartiQL, `JSON_PARSE` |
| Arrays in Athena | `CROSS JOIN UNNEST(...) AS t(x)` |
| Repeated heavy aggregation | Redshift materialized view (auto refresh) |
| View over Spectrum tables | Late-binding view (`WITH NO SCHEMA BINDING`) |
| Multi-step ELT in Redshift | Stored procedure (`LANGUAGE plpgsql`, `CALL`) |
| Earlier steps must persist on failure | `NONATOMIC` procedure |
| Custom logic after Python UDF EOL | SQL UDF / Lambda UDF |
| Cut Athena cost | Partition filter + column pruning + Parquet |
| Function on sort/partition key | Non-sargable — rewrite as a range |
| Dedupe not needed | `UNION ALL` |
| Huge distinct count, error OK | `approx_distinct` / `APPROXIMATE COUNT(DISTINCT)` |
| Inspect a plan | `EXPLAIN` (`EXPLAIN ANALYZE` in Athena) |

SQL answers the "what"; making it visible to the business is the last mile — continue with [Guide 35 — Analytics, Visualization & Notebooks](35-Analytics-Visualization-Quick-Notebooks.md).
