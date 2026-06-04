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
