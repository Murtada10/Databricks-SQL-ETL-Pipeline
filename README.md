# Databricks-SQL-ETL-Pipeline

# Cloud Databricks to SQL Server ETL Pipeline (Healthcare Lab Data Integration)

## 1. Project Objective & Context
End-to-end automated ETL pipeline extracting laboratory requisition records from a cloud Databricks Lakehouse and loading them into an on-premises enterprise SQL Server reporting warehouse for executive Power BI analytics.

## 2. Architecture & Engineering Highlights
* **Source:** Cloud Databricks Lakehouse (relational join across laboratory requisitions and clinical investigation tables).
* **Engine:** Python 3.12 utilizing 50,000-row chunked streaming (`fetchmany`) and `pyodbc` `fast_executemany` bulk inserts to process **1.45M+ records** with minimal memory overhead.
* **Authentication:** OAuth 2.0 Service Principal token generation via REST API with environment-isolated credentials.
* **Schema & Encoding Resolution:** Resolved cross-platform `BigInt`-to-`String` join mismatches (`TRIM(CAST(... AS STRING))`) and UTF-8 Arabic character encoding issues.
* **Presentation & Automation:** Standardized business-logic reporting view orchestrated daily via automated SQL Server Agent jobs.
