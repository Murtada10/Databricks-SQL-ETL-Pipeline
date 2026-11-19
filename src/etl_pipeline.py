"""Cloud Databricks to SQL Server ETL Pipeline."""
import datetime
import logging
import os
import sys
from typing import Optional

import pyodbc
import requests
from databricks import sql
from dotenv import load_dotenv

# ==========================================
# Logging & Configuration
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

DATABRICKS_SERVER = os.getenv("DATABRICKS_SERVER")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")

SQL_SERVER_DRIVER = os.getenv("SQL_SERVER_DRIVER", "ODBC Driver 17 for SQL Server")
SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST")
SQL_SERVER_DB = os.getenv("SQL_SERVER_DB")
SQL_SERVER_USER = os.getenv("SQL_SERVER_USER")
SQL_SERVER_PASSWORD = os.getenv("SQL_SERVER_PASSWORD")

CHUNK_SIZE = int(os.getenv("ETL_CHUNK_SIZE", "50000"))
DEFAULT_START_DATE = os.getenv("ETL_DEFAULT_START_DATE", "2026-01-01")


def validate_config() -> None:
    """Ensure all required environment variables are present before execution."""
    required = [
        "DATABRICKS_SERVER",
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
        "SQL_SERVER_HOST",
        "SQL_SERVER_DB",
        "SQL_SERVER_USER",
        "SQL_SERVER_PASSWORD",
    ]
    missing = [var for var in required if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")


def parse_start_date(cli_arg: Optional[str]) -> str:
    """Validate ISO-8601 date input to prevent malformed queries or SQL injection."""
    target_date = cli_arg if cli_arg else DEFAULT_START_DATE
    try:
        datetime.date.fromisoformat(target_date)
    except ValueError as exc:
        raise ValueError(f"Invalid date format '{target_date}'. Expected YYYY-MM-DD.") from exc
    return target_date


def get_databricks_oauth_token() -> str:
    """Request an OAuth 2.0 access token via Service Principal client credentials."""
    token_url = f"https://{DATABRICKS_SERVER}/oidc/v1/token"
    response = requests.post(
        token_url,
        auth=(DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET),
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        timeout=30,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("OAuth response did not include an access_token.")
    return token


def build_sql_server_conn_str() -> str:
    return (
        f"DRIVER={{{SQL_SERVER_DRIVER}}};"
        f"SERVER={SQL_SERVER_HOST};"
        f"DATABASE={SQL_SERVER_DB};"
        f"UID={SQL_SERVER_USER};"
        f"PWD={SQL_SERVER_PASSWORD};"
    )


def run_etl_pipeline() -> None:
    validate_config()
    start_date = parse_start_date(sys.argv[1] if len(sys.argv) > 1 else None)
    logger.info("Starting ETL Pipeline | Target start date: %s | Chunk size: %s", start_date, CHUNK_SIZE)

    # Type casting resolves BigInt-to-String join mismatches and trailing whitespace
    extract_query = """
        SELECT
            lab.CandidateID AS CandidateNID,
            lab.InvestigationID,
            inv.InvestigationCode AS InvestigationCode,
            lab.CreatedDate AS InvestigationDate,
            lab.RequisitionCode AS RequisitionCode,
            lab.PhysicianRequestDate AS PhysicianRequestDate,
            lab.RequisitionSpecimenID AS SampleID,
            lab.TestStatus AS TestStatus,
            lab.TestName,
            lab.Disease
        FROM enterprise_lakehouse.clinical_reports.lab_requisitions lab
        LEFT JOIN enterprise_lakehouse.clinical_reports.clinical_investigations inv
            ON TRIM(CAST(lab.InvestigationID AS STRING)) = TRIM(CAST(inv.InvestigationID AS STRING))
        WHERE lab.CreatedDate >= ?
          AND lab.RequisitionSpecimenID IS NOT NULL
    """

    insert_query = """
        INSERT INTO dbo.Staging_Lab_Requisitions (
            CandidateNID,
            InvestigationID,
            InvestigationCode,
            InvestigationDate,
            RequisitionCode,
            PhysicianRequestDate,
            SampleID,
            TestStatus,
            TestName,
            Disease
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    try:
        logger.info("Authenticating with Databricks via OAuth 2.0 Service Principal...")
        access_token = get_databricks_oauth_token()

        logger.info("Connecting to Databricks Lakehouse and streaming source dataset...")
        with sql.connect(
            server_hostname=DATABRICKS_SERVER,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=access_token,
        ) as db_conn, db_conn.cursor() as db_cursor:
            db_cursor.execute(extract_query, parameters=[start_date])

            with pyodbc.connect(build_sql_server_conn_str()) as sql_conn:
                sql_cursor = sql_conn.cursor()

                logger.info("Truncating target staging table...")
                sql_cursor.execute("TRUNCATE TABLE dbo.Staging_Lab_Requisitions;")
                sql_conn.commit()

                sql_cursor.fast_executemany = True
                total_inserted = 0

                while True:
                    rows = db_cursor.fetchmany(CHUNK_SIZE)
                    if not rows:
                        break

                    chunk_data = [
                        (
                            row[0],
                            row[1],
                            row[2],
                            row[3],
                            row[4],
                            row[5],
                            str(row[6]).strip() if row[6] is not None else None,
                            row[7],
                            row[8],
                            row[9],
                        )
                        for row in rows
                    ]

                    sql_cursor.executemany(insert_query, chunk_data)
                    sql_conn.commit()
                    total_inserted += len(chunk_data)
                    logger.info("Streamed and loaded %s records...", f"{total_inserted:,}")

        logger.info("ETL completed successfully. Total records loaded: %s", f"{total_inserted:,}")

    except Exception:
        logger.exception("ETL pipeline failed during execution.")
        sys.exit(1)


if __name__ == "__main__":
    run_etl_pipeline()
