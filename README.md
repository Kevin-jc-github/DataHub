# DataHubX

DataHubX is a production-grade data platform that simulates a real-world enterprise data infrastructure. It supports hybrid real-time and batch data processing, data lake architecture, data quality governance, workflow orchestration, and business-level dashboards.

## 🌐 Architecture Overview

- **Ingestion Layer**: Real-time data ingestion from Kafka, CDC tools like Debezium or Airbyte.
- **Streaming Layer**: Real-time processing using Apache Flink for user behavior metrics and event joins.
- **Batch Layer**: Scheduled batch jobs using Apache Spark for daily aggregation and warehouse updates.
- **Warehouse**: Layered modeling (ODS → DWD → DWS/ADS) in Hive with Hudi/Iceberg for incremental data.
- **Orchestration**: Apache Airflow for DAG dependency, SLA, retries, and failure handling.
- **Data Quality**: Great Expectations rules embedded into workflows for schema checks, null ratio, etc.
- **Lineage**: Apache Atlas or metadata graphs for end-to-end traceability.
- **APIs & BI Dashboards**: FastAPI to serve key metrics; Superset for dashboards and exploration.
- **DevOps**: Dockerized deployment with GitHub Actions for CI/CD workflows.

## 📁 Directory Structure
<pre>
DataHubX/
├── ingestion/         → Kafka producers, CDC, Airbyte
├── streaming/         → Flink + CEP
├── batch/             → Spark with Hudi/Iceberg
├── warehouse/         → dbt or Hive SQL models
├── airflow/           → DAGs + configs
├── quality/           → Great Expectations
├── lineage/           → Apache Atlas
├── api/               → FastAPI service
├── dashboards/        → Superset/Metabase charts
├── deploy/            → Docker + monitoring
├── llm_module/        → GPT / 文心一言行为分析
├── security/          → Apache Ranger
├── tests/             → 单元测试和 CI/CD
└── README.md          → 项目文档

</pre>

## 📌 How to Start

This project will evolve over 4 stages:
1. Data Ingestion & Kafka Simulation
2. Real-time + Batch Layer and Data Lake Design
3. Airflow DAGs + Data Governance
4. Dashboard Building + API Exposure + CI/CD

Stay tuned for updates in each module.

