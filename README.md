# Cloud Databricks to SQL Server ETL Pipeline

> **Enterprise Confidentiality Notice (NDA):** This repository presents the reference architecture and a fully sanitized implementation of a production healthcare data integration pipeline. All proprietary server hostnames, credentials, schema identifiers, and organization-specific names have been redacted and replaced with generic placeholders.

## 1. Project Objective & Context
An automated, end-to-end ETL pipeline designed to extract **1.45M+ clinical laboratory requisition records** from a cloud **Databricks Lakehouse** and load them into an on-premises **SQL Server** reporting warehouse powering executive **Power BI** dashboards.

The pipeline bridges historical granularity gaps between clinical investigation records and Laboratory Information System (LIS) reporting requirements, delivering clean, standardized datasets on a daily schedule.

## 2. System Architecture

```mermaid
flowchart LR
    subgraph Cloud ["Cloud Lakehouse (Databricks)"]
        A[("Lab Requisitions &<br/>Investigations Tables")]
        B["OAuth 2.0<br/>Service Principal"]
    end

    subgraph ETL ["Python 3.12 Streaming Engine"]
        C["Cross-Table Join &<br/>Schema Casting"]
        D["50,000-Row Chunked<br/>Streaming (fetchmany)"]
        E["Bulk Load Engine<br/>pyodbc fast_executemany"]
    end

    subgraph OnPrem ["On-Premises SQL Server DW"]
        F[("Staging Table<br/>Truncate & Load")]
        G["Presentation View<br/>Status Standardization"]
        H["SQL Server Agent<br/>Daily CmdExec Job"]
    end

    I["Executive Power BI<br/>Dashboards"]

    B -->|Bearer Token| C
    A --> C --> D --> E --> F --> G --> I
    H -.->|Triggers Daily| C
