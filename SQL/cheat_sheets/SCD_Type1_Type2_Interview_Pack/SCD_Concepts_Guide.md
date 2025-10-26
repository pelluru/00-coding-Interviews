# SCD Type 1 & Type 2 — Concepts, Q&A, and Practicals

## 📘 Concept Summary
**SCD Type 1**: Overwrites old data — no history.  
**SCD Type 2**: Inserts new record for each change — history preserved.

### Example Difference

| Type | Behavior | Use Case |
|------|-----------|----------|
| SCD1 | Overwrite current row | Fix errors |
| SCD2 | Add new version | Track history |

---

## ⚙️ SQL Examples

### 🔹 Type 1
```sql
merge into dim_customer t
using stg_customer s
on t.customer_id = s.customer_id
when matched then
  update set name = s.name, city = s.city
when not matched then
  insert (customer_id, name, city)
  values (s.customer_id, s.name, s.city);
```

### 🔹 Type 2
```sql
merge into dim_customer t
using stg_customer s
on t.customer_id = s.customer_id and t.is_current = true
when matched and (s.city <> t.city or s.name <> t.name) then
  update set t.end_date = current_date - 1, t.is_current = false
when not matched by target then
  insert (customer_id, name, city, start_date, end_date, is_current)
  values (s.customer_id, s.name, s.city, current_date, '9999-12-31', true);
```

---

## 💬 Interview Questions and Answers

1. **What’s the key difference between SCD1 and SCD2?**  
   → Type 1 overwrites data; Type 2 preserves history with effective dates.

2. **When is SCD1 preferred?**  
   → When data corrections are needed and historical tracking is irrelevant.

3. **What columns are required for SCD2?**  
   → `start_date`, `end_date`, `is_current`, and a surrogate key.

4. **How do you detect changes efficiently?**  
   → Compare MD5 hash of tracked columns.

5. **How do you query data as of a date?**  
   ```sql
   select * from dim_customer
   where '2024-03-01' between start_date and end_date;
   ```

6. **How does dbt support SCD2?**  
   → With incremental models using `merge` strategy, `unique_key`, and Jinja logic.

7. **What’s SCD3?**  
   → Stores limited history (current and previous values only).

8. **How to handle late-arriving data in SCD2?**  
   → Adjust start/end dates to fit actual effective time and reopen closed records.

9. **Common SCD2 issues?**  
   → Overlapping dates, duplicate keys, performance with merges.

10. **How to optimize SCD2 merges?**  
    → Use hash comparisons, incremental staging, and partition pruning.

---

## 🧪 Practical Scenarios

### Create Table
```sql
create table dim_customer (
  sk_customer serial primary key,
  customer_id int,
  name text,
  city text,
  start_date date,
  end_date date,
  is_current boolean
);
```

### Insert Initial
```sql
insert into dim_customer
(customer_id, name, city, start_date, end_date, is_current)
values (1,'Alex','LA','2024-01-01','9999-12-31',true);
```

### Apply Change (SCD2)
```sql
update dim_customer
set end_date = current_date - 1, is_current = false
where customer_id = 1 and is_current = true;

insert into dim_customer
(customer_id, name, city, start_date, end_date, is_current)
values (1,'Alex','SF',current_date,'9999-12-31',true);
```

---

## 🧱 dbt Incremental Example
```jinja
{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
) }}

select
  customer_id,
  name,
  city,
  current_timestamp() as load_ts
from {{ ref('stg_customer') }}
```
