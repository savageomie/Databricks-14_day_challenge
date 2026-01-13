# Day 5 (13/01/26) — Delta Lake Advanced ✅

## 📌 Overview
On Day 5, I learned and implemented **advanced Delta Lake operations** in Databricks.  
Since my Databricks workspace has **Public DBFS root disabled**, I avoided saving Delta files to `/delta/...`, `/tmp/...`, `/FileStore/...` paths and instead completed the assignment using **Managed Delta Tables** in the `workspace` catalog.

---

## ✅ Topics Learned
- **MERGE operations** (Upsert / incremental updates)
- **Time Travel** (query older versions of Delta tables)
- Performance optimization using **OPTIMIZE**
- Data skipping improvement using **ZORDER**
- Cleanup using **VACUUM**

---

## 🛠️ Tasks Completed

### ✅ 1) Create Initial Delta Table (Events Table)
Created an events dataset and stored it as a **Managed Delta Table**.

Table created:
- `workspace.default.events_table_day5`

✅ Proof:
- Table preview + ordered output by `event_time`

---

### ✅ 2) Implement Incremental MERGE (Upsert)
Created incremental update data containing:
- **One matching record** (same `user_session` + `event_time`) → updated
- **One new record** → inserted

MERGE condition used:
- `t.user_session = s.user_session AND t.event_time = s.event_time`

MERGE actions:
- `WHEN MATCHED THEN UPDATE SET *`
- `WHEN NOT MATCHED THEN INSERT *`

✅ Proof:
- Updated record reflected in table
- New record inserted successfully

---

### ✅ 3) Query Historical Versions (Time Travel)
Checked Delta version history using:
- `DESCRIBE HISTORY events_table_day5`

Queried older snapshot using time travel:
- `SELECT * FROM events_table_day5 VERSION AS OF 0`

✅ Proof:
- History output showing versions created by operations
- Version 0 snapshot output

---

### ✅ 4) Optimize Table (OPTIMIZE + ZORDER)
Optimized table storage layout for faster queries using:

- `OPTIMIZE events_table_day5`
- `ZORDER BY (event_type, user_id)`

✅ Proof:
- OPTIMIZE operation visible in `DESCRIBE HISTORY`

---

### ✅ 5) Clean Old Files (VACUUM)
Cleaned unused / stale Delta files using:

- `VACUUM events_table_day5 RETAIN 168 HOURS`

✅ Proof:
- VACUUM command output
- VACUUM operation visible in Delta history

---

## 📂 Tables Created
| Table Name | Type | Description |
|-----------|------|-------------|
| `workspace.default.events_table_day5` | Delta Managed Table | Events Delta table used for MERGE, Time Travel, OPTIMIZE/ZORDER and VACUUM |

---

## 📸 Screenshots Collected
- Initial Delta table preview
- MERGE result showing update + insert
- `DESCRIBE HISTORY` output
- Time Travel result (`VERSION AS OF 0`)
- OPTIMIZE output / history entry
- VACUUM output / history entry

---

## ✅ Conclusion
Day 5 successfully covered **Delta Lake advanced concepts** with hands-on implementation of:
- Incremental updates using **MERGE**
- Historical querying using **Time Travel**
- Performance improvements with **OPTIMIZE & ZORDER**
- Cleanup using **VACUUM**

All tasks completed successfully ✅

