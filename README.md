# 📈 Real-Time Market Data ELT Pipeline

A production-oriented ELT pipeline that ingests 1-minute OHLCV market data from Yahoo Finance, stores it in PostgreSQL, and transforms it using dbt.

---

## 🚀 What This Project Demonstrates

- Real-time data ingestion with 1-minute market data
- Modular Python collector architecture
- Airflow orchestration
- PostgreSQL raw data storage
- dbt-based staging layer with tests and data contracts
- Production-style project structure

---

## 🏗 Architecture Overview

```text
Yahoo Finance (yfinance)
        ↓
Python Collector (elt/collector)
        ↓
PostgreSQL (public.market_data)  ← Raw Layer
        ↓
dbt Staging (staging.stg_market_data)
        ↓
Ready for marts / analytics
```

Airflow DAG: `market_data_yahoo_minutely`  
Schedule: Every 1 minute

---

## 🔧 Tech Stack

- Python 3.10
- yfinance
- PostgreSQL
- Apache Airflow
- dbt with Postgres adapter
- Virtualenv
- Git

---

## 📂 Project Structure

```text
elt/
 ├── collector/
 │    ├── collectors/yahoo.py
 │    ├── storage/db.py
 │    ├── storage/writer.py
 │    ├── job.py
 │    ├── config.py
 │    └── utils/logger.py
 │
 ├── airflow/
 │    └── dags/
 │         └── market_data_yahoo_minutely.py
 │
 ├── dbt/
 │    ├── models/staging/
 │    │    ├── _sources.yml
 │    │    ├── stg_market_data.sql
 │    │    └── stg_market_data.yml
 │    ├── dbt_project.yml
 │    └── packages.yml
 │
 └── sql/
```

---

## 🗄 Database Schema: Raw Layer

Table: `public.market_data`

| Column | Type | Constraint |
|---|---|---|
| symbol | text | not null |
| datetime | timestamptz | not null |
| open | double precision |  |
| high | double precision |  |
| low | double precision |  |
| close | double precision |  |
| volume | bigint |  |
| Primary key | symbol, datetime | unique row identifier |

---

## 🔄 dbt Staging Layer

Location: `elt/dbt`

Model: `staging.stg_market_data`

Transformations:

- Explicit column selection
- Type casting enforcement
- Rename `datetime` to `datetime_utc`
- Data contract layer for downstream analytics

Tests:

- `not_null(symbol)`
- `not_null(datetime_utc)`
- `unique(symbol, datetime_utc)`

Run dbt models and tests:

```bash
cd elt/dbt
dbt run --select staging
dbt test --select staging
```

---

## ⚙️ Setup

### 1. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment variables

Create a `.env` file based on `.env.example` and update the required values:

```bash
cp .env.example .env
```

### 3. Start Airflow

```bash
airflow webserver
airflow scheduler
```

Enable and run the DAG from the Airflow UI:

```text
market_data_yahoo_minutely
```

---

## 🧱 Production-Oriented Design

- Separation of ingestion and transformation
- Airflow-managed scheduling with retries
- Modular collector architecture
- PostgreSQL raw layer for persistent storage
- dbt staging layer for clean and tested data models
- Explicit data contracts in staging
- Test-driven data validation
- Version-controlled transformations

---

## 📊 Future Extensions

- Incremental dbt models
- Aggregated data marts
- Feature engineering layer in a separate repository
- Dockerization
- CI/CD for dbt tests
- Monitoring and alerting
