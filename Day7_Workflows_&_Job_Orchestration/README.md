# Day 7 — Databricks Jobs & Multi-Task Workflows ✅

## 📌 Overview
On Day 7, I explored how to automate pipelines in Databricks using **Jobs** and **multi-task workflows**.  
Instead of running notebooks manually, I learned how to schedule executions, pass parameters dynamically, and handle failures using Databricks workflow features.

---

## 🎯 Learning Goals
- Understand the difference between **Databricks Jobs vs Notebooks**
- Learn how **Multi-task workflows** work (task chaining)
- Use **Parameters & scheduling** for automation
- Implement **error handling** and failure recovery strategies

---

## 🧠 Key Concepts Learned

### ✅ Databricks Notebooks
- Interactive development environment
- Used for exploration, development, debugging
- Runs manually unless triggered via Job

### ✅ Databricks Jobs
- Used for automation & production execution
- Can run notebooks, scripts, SQL, pipelines
- Supports:
  - Scheduling
  - Retry policies
  - Alerts
  - Dependencies between tasks
  - Parameters

### ✅ Multi-Task Workflows
- A workflow can contain multiple tasks
- Tasks can run sequentially or in parallel
- Enables real-world pipelines like:
  - Bronze → Silver → Gold

---

## 🛠️ Tasks Completed (Day 7)

### 1) Add Parameter Widgets to Notebooks
- Added **Databricks widgets** for runtime parameters such as:
  - input path
  - table name
  - load date
  - run mode (full/incremental)

✅ This makes notebooks reusable and dynamic inside workflows.

---

### 2) Create a Multi-Task Job (Bronze → Silver → Gold)
Created a Databricks **multi-task job** to execute notebooks in pipeline order:

1. **Bronze notebook**
   - Ingest raw data
   - Store as Bronze table

2. **Silver notebook**
   - Clean + transform data
   - Store curated output as Silver table

3. **Gold notebook**
   - Create analytics-ready aggregated dataset
   - Store Gold output table

---

### 3) Set Up Dependencies
Configured workflow dependencies:

- **Silver task depends on Bronze**
- **Gold task depends on Silver**

✅ Ensures correct execution order and prevents invalid pipeline runs.

---

### 4) Schedule Execution
Scheduled the workflow to run automatically using Databricks Jobs scheduler.

Included:
- start time
- frequency (daily/weekly as per requirement)
- timezone
- optional pause/resume controls

✅ This enables hands-free automation of the full pipeline.

---

## ⚙️ Error Handling Strategy
Configured workflow settings for reliability:
- **Retries** on failure (to recover from temporary issues)
- **Timeouts** (to prevent infinite execution)
- **Task failure handling**
  - Stop downstream tasks if upstream fails
  - Track execution logs for debugging

---

## 📂 Deliverables / Evidence
- Parameterized notebooks (Bronze, Silver, Gold)
- Databricks Workflow Job:
  - Multi-task pipeline
  - Dependencies configured
  - Scheduled execution enabled

---

## ✅ Outcome
By completing Day 7, I learned how to convert manual notebook execution into a fully automated **Databricks Job Workflow**, including parameter passing, dependency management, scheduling, and error handling.

---
# Day 7 — Databricks Jobs & Multi-Task Workflows ✅

## 📌 Overview
On Day 7, I explored how to automate pipelines in Databricks using **Jobs** and **multi-task workflows**.  
Instead of running notebooks manually, I learned how to schedule executions, pass parameters dynamically, and handle failures using Databricks workflow features.

---

## 🎯 Learning Goals
- Understand the difference between **Databricks Jobs vs Notebooks**
- Learn how **Multi-task workflows** work (task chaining)
- Use **Parameters & scheduling** for automation
- Implement **error handling** and failure recovery strategies

---

## 🧠 Key Concepts Learned

### ✅ Databricks Notebooks
- Interactive development environment
- Used for exploration, development, debugging
- Runs manually unless triggered via Job

### ✅ Databricks Jobs
- Used for automation & production execution
- Can run notebooks, scripts, SQL, pipelines
- Supports:
  - Scheduling
  - Retry policies
  - Alerts
  - Dependencies between tasks
  - Parameters

### ✅ Multi-Task Workflows
- A workflow can contain multiple tasks
- Tasks can run sequentially or in parallel
- Enables real-world pipelines like:
  - Bronze → Silver → Gold

---

## 🛠️ Tasks Completed (Day 7)

### 1) Add Parameter Widgets to Notebooks
- Added **Databricks widgets** for runtime parameters such as:
  - input path
  - table name
  - load date
  - run mode (full/incremental)

✅ This makes notebooks reusable and dynamic inside workflows.

---

### 2) Create a Multi-Task Job (Bronze → Silver → Gold)
Created a Databricks **multi-task job** to execute notebooks in pipeline order:

1. **Bronze notebook**
   - Ingest raw data
   - Store as Bronze table

2. **Silver notebook**
   - Clean + transform data
   - Store curated output as Silver table

3. **Gold notebook**
   - Create analytics-ready aggregated dataset
   - Store Gold output table

---

### 3) Set Up Dependencies
Configured workflow dependencies:

- **Silver task depends on Bronze**
- **Gold task depends on Silver**

✅ Ensures correct execution order and prevents invalid pipeline runs.

---

### 4) Schedule Execution
Scheduled the workflow to run automatically using Databricks Jobs scheduler.

Included:
- start time
- frequency (daily/weekly as per requirement)
- timezone
- optional pause/resume controls

✅ This enables hands-free automation of the full pipeline.

---

## ⚙️ Error Handling Strategy
Conf# Day 7 — Databricks Jobs & Multi-Task Workflows ✅

## 📌 Overview
On Day 7, I explored how to automate pipelines in Databricks using **Jobs** and **multi-task workflows**.  
Instead of running notebooks manually, I learned how to schedule executions, pass parameters dynamically, and handle failures using Databricks workflow features.

---

## 🎯 Learning Goals
- Understand the difference between **Databricks Jobs vs Notebooks**
- Learn how **Multi-task workflows** work (task chaining)
- Use **Parameters & scheduling** for automation
- Implement **error handling** and failure recovery strategies

---

## 🧠 Key Concepts Learned

### ✅ Databricks Notebooks
- Interactive development environment
- Used for exploration, development, debugging
- Runs manually unless triggered via Job

### ✅ Databricks Jobs
- Used for automation & production execution
- Can run notebooks, scripts, SQL, pipelines
- Supports:
  - Scheduling
  - Retry policies
  - Alerts
  - Dependencies between tasks
  - Parameters

### ✅ Multi-Task Workflows
- A workflow can contain multiple tasks
- Tasks can run sequentially or in parallel
- Enables real-world pipelines like:
  - Bronze → Silver → Gold

---

## 🛠️ Tasks Completed (Day 7)

### 1) Add Parameter Widgets to Notebooks
- Added **Databricks widgets** for runtime parameters such as:
  - input path
  - table name
  - load date
  - run mode (full/incremental)

✅ This makes notebooks reusable and dynamic inside workflows.

---

### 2) Create a Multi-Task Job (Bronze → Silver → Gold)
Created a Databricks **multi-task job** to execute notebooks in pipeline order:

1. **Bronze notebook**
   - Ingest raw data
   - Store as Bronze table

2. **Silver notebook**
   - Clean + transform data
   - Store curated output as Silver table

3. **Gold notebook**
   - Create analytics-ready aggregated dataset
   - Store Gold output table

---

### 3) Set Up Dependencies
Configured workflow dependencies:

- **Silver task depends on Bronze**
- **Gold task depends on Silver**

✅ Ensures correct execution order and prevents invalid pipeline runs.

---

### 4) Schedule Execution
Scheduled the workflow to run automatically using Databricks Jobs scheduler.

Included:
- start time
- frequency (daily/weekly as per requirement)
- timezone
- optional pause/resume controls

✅ This enables hands-free automation of the full pipeline.

---

## ⚙️ Error Handling Strategy
Configured workflow settings for reliability:
- **Retries** on failure (to recover from temporary issues)
- **Timeouts** (to prevent infinite execution)
- **Task failure handling**
  - Stop downstream tasks if upstream fails
  - Track execution logs for debugging

---

## 📂 Deliverables / Evidence
- Parameterized notebooks (Bronze, Silver, Gold)
- Databricks Workflow Job:
  - Multi-task pipeline
  - Dependencies configured
  - Scheduled execution enabled

---

## ✅ Outcome
By completing Day 7, I learned how to convert manual notebook execution into a fully automated **Databricks Job Workflow**, including parameter passing, dependency management, scheduling, and error handling.

---
igured workflow settings for reliability:
- **Retries** on failure (to recover from temporary issues)
- **Timeouts** (to prevent infinite execution)
- **Task failure handling**
  - Stop downstream tasks if upstream fails
  - Track execution logs for debugging

---

## 📂 Deliverables / Evidence
- Parameterized notebooks (Bronze, Silver, Gold)
- Databricks Workflow Job:
  - Multi-task pipeline
  - Dependencies configured
  - Scheduled execution enabled

---

## ✅ Outcome
By completing Day 7, I learned how to convert manual notebook execution into a fully automated **Databricks Job Workflow**, including parameter passing, dependency management, scheduling, and error handling.

---
