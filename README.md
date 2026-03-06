# Payment Anomaly Detector

> **Status: Work in Progress** — This project is actively under development. Currently on Step 1: repository setup and folder structure.

A end-to-end data pipeline that detects anomalies in payment transaction metrics, drills down into contributing dimensions, and uses an LLM to generate human-readable explanations. Built with PySpark, DuckDB, Airflow, and Streamlit.

## Architecture Overview

```
Raw Transactions (5M+)
        |
   [ PySpark Aggregation ]
        |
   Daily Metrics by Dimension
        |
   [ Anomaly Detection (Z-score) ]
        |
   [ DuckDB Drill-Down ]
        |
   [ Claude LLM Hypothesis Generator ]
        |
   [ Streamlit Dashboard ]
```

## Planned Components

| # | Component | Description | Status |
|---|-----------|-------------|--------|
| 1 | **Project Setup** | Git repo, `.gitignore`, Docker/docker-compose (Spark, Airflow, DuckDB), folder structure | In Progress |
| 2 | **Data Generation** | PySpark script to generate ~5M synthetic transactions with 3-4 seeded anomalies | Not Started |
| 3 | **CI/CD** | GitHub Actions for pytest and linting, branch protection rules | Not Started |
| 4 | **Aggregation Layer** | PySpark job: raw transactions to daily metrics by dimension | Not Started |
| 5 | **Anomaly Detection** | Python module using Z-score / deviation detection | Not Started |
| 6 | **Dimension Drill-Down** | DuckDB queries to slice anomalies by dimension | Not Started |
| 7 | **LLM Hypothesis Generator** | Claude API integration to generate explanations for detected anomalies | Not Started |
| 8 | **Streamlit Dashboard** | Visualize metrics, anomalies, and LLM-generated explanations | Not Started |
| 9 | **Orchestration** | Airflow DAG tying steps 4-7 together | Not Started |

## Tech Stack

- **Processing**: PySpark
- **Anomaly Detection**: Python (Z-score / statistical methods)
- **Analytical Queries**: DuckDB
- **LLM**: Claude API (Anthropic)
- **Dashboard**: Streamlit
- **Orchestration**: Apache Airflow
- **Infrastructure**: Docker, docker-compose

## Getting Started

> Setup instructions will be added as the project progresses.

## Project Structure

```
payment-anomaly-detector/
├── README.md
├── project_plan.txt
├── requirements.txt
├── .gitignore
├── docker-compose.yml          # (planned)
├── dags/                       # Airflow DAGs (planned)
├── src/
│   ├── data_generation/        # Synthetic transaction generator (planned)
│   ├── aggregation/            # PySpark aggregation jobs (planned)
│   ├── anomaly_detection/      # Z-score detection module (planned)
│   ├── drill_down/             # DuckDB dimension queries (planned)
│   └── llm/                    # Claude API integration (planned)
├── dashboard/                  # Streamlit app (planned)
└── tests/                      # Unit and integration tests (planned)
```
