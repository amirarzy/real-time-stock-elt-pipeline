# 📈 Real-Time Market Data ELT Pipeline

A production-oriented ELT pipeline that ingests 1-minute OHLCV market data from Yahoo Finance, stores it in PostgreSQL, and transforms it using dbt.

---

## 🚀 What This Project Demonstrates

- Real-time data ingestion (1-minute interval)
- Modular Python collector architecture
- Airflow orchestration
- PostgreSQL storage
- dbt-based staging layer with tests and data contracts
- Production-style project structure

---

## 🏗 Architecture Overview

Yahoo Finance (yfinance)
        ↓
Python Collector (elt/collector)
        ↓
PostgreSQL (public.market_data)  ← Raw Layer
        ↓
dbt Staging (staging.stg_market_data)
        ↓
Ready for marts / analytics / ML

Airflow DAG: `market_data_yahoo_minutely`  
Schedule: Every 1 minute

---

## 🔧 Tech Stack

- Python 3.10
- yfinance
- PostgreSQL
- Apache Airflow
- dbt (Postgres adapter)
- Virtualenv
- Git

---

## 📂 Project Structure

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

---

## 🗄 Database Schema (Raw Layer)

Table: `public.market_data`

- symbol (text, not null)
- datetime (timestamptz, not null)
- open (double precision)
- high (double precision)
- low (double precision)
- close (double precision)
- volume (bigint)
- Primary key (symbol, datetime)

---

## 🔄 dbt Staging Layer

Location: `elt/dbt`

Model: `staging.stg_market_data`

Transformations:
- Explicit column selection
- Type casting enforcement
- Rename `datetime` → `datetime_utc`
- Data contract layer

Tests:
- not_null(symbol)
- not_null(datetime_utc)
- unique(symbol, datetime_utc)

Run:
dbt run --select staging
dbt test --select staging

---

## ⚙️ Setup

1️⃣ Create virtual environment

python -m venv .venv  
source .venv/bin/activate  
pip install -r requirements.txt  

2️⃣ Configure environment variables in `.env`

3️⃣ Start Airflow

airflow webserver  
airflow scheduler  

---

## 🧱 Production-Oriented Design

- Separation of ingestion and transformation
- Airflow-managed scheduling with retries
- Modular collector architecture
- Explicit data contracts in staging
- Test-driven data validation
- Version-controlled transformations

---

## 📊 Future Extensions

- Incremental dbt models
- Aggregated data marts
- Feature engineering layer
- ML forecasting models
- Dockerization
- CI/CD for dbt tests
- Monitoring & alerting
