from fastapi import FastAPI
import boto3
import time
import os
import json

from datetime import datetime, date, timedelta
from botocore.exceptions import ClientError

from datasets import DATASETS

# =========================================================
# FASTAPI APP
# =========================================================

app = FastAPI()

# =========================================================
# CONFIGURATION
# =========================================================

REGION = "us-east-1"

DATABASE = "domo_reports_s3"

OUTPUT_BUCKET = "s3://keynes-athena-results-srinivas"

MAX_QUERY_LENGTH = 5000

LOG_DIR = "logs"

LOG_FILE = f"{LOG_DIR}/query_history.log"

ALLOWED_TABLES = [

    "client_reporting_date_dataset",

    "client_reporting_geo_dataset",

    "client_reporting_network_dataset_myreports",

    "client_reporting_hour_dataset"

]

BLOCKED_KEYWORDS = [

    "DROP",
    "DELETE",
    "TRUNCATE",
    "ALTER",
    "INSERT",
    "UPDATE",
    "CREATE"

]

# =========================================================
# CREATE LOG DIRECTORY
# =========================================================

os.makedirs(LOG_DIR, exist_ok=True)

# =========================================================
# ATHENA CLIENT
# =========================================================

athena = boto3.client(

    "athena",

    region_name=REGION

)

# =========================================================
# QUERY LOGGER
# =========================================================

def log_query(entry: dict):

    with open(LOG_FILE, "a") as f:

        f.write(
            json.dumps(entry) + "\n"
        )

# =========================================================
# SQL VALIDATION
# =========================================================

def validate_sql(sql: str):

    sql_upper = sql.upper()

    # =====================================================
    # QUERY LENGTH CHECK
    # =====================================================

    if len(sql) > MAX_QUERY_LENGTH:

        return {

            "valid": False,

            "error":
            "Query exceeds maximum allowed length"

        }

    # =====================================================
    # ALLOWED QUERY TYPES
    # =====================================================

    allowed_starts = [

        "SELECT",
        "SHOW",
        "WITH"

    ]

    if not any(
        sql_upper.strip().startswith(x)
        for x in allowed_starts
    ):

        return {

            "valid": False,

            "error":
            "Only SELECT/SHOW/WITH queries allowed"

        }

    # =====================================================
    # BLOCK DANGEROUS KEYWORDS
    # =====================================================

    for keyword in BLOCKED_KEYWORDS:

        if keyword in sql_upper:

            return {

                "valid": False,

                "error":
                f"Blocked keyword detected: {keyword}"

            }

    # =====================================================
    # ALLOWED TABLES
    # =====================================================

    found_allowed_table = False

    for table in ALLOWED_TABLES:

        if table.lower() in sql.lower():

            found_allowed_table = True
            break

    if sql_upper.startswith("SHOW COLUMNS"):
        found_allowed_table = True

    if not found_allowed_table:

        return {

            "valid": False,

            "error":
            "Unauthorized table access"

        }

    # =====================================================
    # AUTO LIMIT
    # =====================================================

    if (
        "LIMIT" not in sql_upper
        and sql_upper.startswith("SELECT")
    ):

        sql += "\nLIMIT 100"

    return {

        "valid": True,

        "sql": sql

    }

# =========================================================
# ATHENA QUERY EXECUTION
# =========================================================

def run_athena_query(query: str):

    start_time = time.time()

    print("\n================================================")
    print("EXECUTING ATHENA QUERY")
    print("================================================")
    print(query)

    validation = validate_sql(query)

    if not validation["valid"]:

        log_query({

            "timestamp":
            str(datetime.utcnow()),

            "success":
            False,

            "query":
            query,

            "error":
            validation["error"]

        })

        return {

            "success": False,

            "error":
            validation["error"],

            "results": []

        }

    query = validation["sql"]

    try:

        response = athena.start_query_execution(

            QueryString=query,

            QueryExecutionContext={
                "Database": DATABASE
            },

            ResultConfiguration={
                "OutputLocation": OUTPUT_BUCKET
            }

        )

        query_execution_id = response[
            "QueryExecutionId"
        ]

        while True:

            status = athena.get_query_execution(

                QueryExecutionId=query_execution_id

            )

            state = status[
                "QueryExecution"
            ]["Status"]["State"]

            print(f"Athena State: {state}")

            if state == "SUCCEEDED":
                break

            if state in ["FAILED", "CANCELLED"]:

                reason = status[
                    "QueryExecution"
                ]["Status"].get(

                    "StateChangeReason",

                    "Unknown Athena Error"

                )

                log_query({

                    "timestamp":
                    str(datetime.utcnow()),

                    "success":
                    False,

                    "query":
                    query,

                    "query_execution_id":
                    query_execution_id,

                    "error":
                    reason

                })

                return {

                    "success": False,

                    "error": reason,

                    "results": []

                }

            time.sleep(2)

        results = athena.get_query_results(

            QueryExecutionId=query_execution_id

        )

        rows = results["ResultSet"]["Rows"]

        if len(rows) <= 1:

            parsed_rows = []

        else:

            headers = [

                col.get("VarCharValue", "")
                for col in rows[0]["Data"]

            ]

            parsed_rows = []

            for row in rows[1:]:

                values = [

                    col.get("VarCharValue", "")
                    for col in row["Data"]

                ]

                parsed_rows.append(

                    dict(zip(headers, values))

                )

        execution_time = round(

            time.time() - start_time,
            2

        )

        log_query({

            "timestamp":
            str(datetime.utcnow()),

            "success":
            True,

            "query":
            query,

            "query_execution_id":
            query_execution_id,

            "row_count":
            len(parsed_rows),

            "execution_time_seconds":
            execution_time

        })

        return {

            "success": True,

            "query_execution_id":
            query_execution_id,

            "row_count":
            len(parsed_rows),

            "execution_time_seconds":
            execution_time,

            "results":
            parsed_rows

        }

    except ClientError as e:

        error_message = str(e)

        log_query({

            "timestamp":
            str(datetime.utcnow()),

            "success":
            False,

            "query":
            query,

            "error":
            error_message

        })

        return {

            "success": False,

            "error":
            error_message,

            "results": []

        }

    except Exception as e:

        error_message = str(e)

        log_query({

            "timestamp":
            str(datetime.utcnow()),

            "success":
            False,

            "query":
            query,

            "error":
            error_message

        })

        return {

            "success": False,

            "error":
            error_message,

            "results": []

        }

# =========================================================
# HOME
# =========================================================

@app.get("/")
def home():

    return {

        "message":
        "Keynes QA Analytics API Running",

        "environment":
        "QA"

    }

# =========================================================
# HEALTH
# =========================================================

@app.get("/health")
def health():

    return {

        "status":
        "healthy",

        "environment":
        "qa"

    }

# =========================================================
# DATASETS
# =========================================================

@app.get("/datasets")
def get_datasets():

    return {

        "available_datasets":
        DATASETS

    }

# =========================================================
# SCHEMA DISCOVERY
# =========================================================

@app.get("/schema/{table_name}")
def get_table_schema(table_name: str):

    query = f"SHOW COLUMNS IN {table_name}"

    return run_athena_query(query)

# =========================================================
# CURRENT DATE
# =========================================================

@app.get("/current-date")
def current_date():

    today = date.today()

    this_week_start = (
        today - timedelta(days=today.weekday())
    )

    last_week_start = (
        this_week_start - timedelta(days=7)
    )

    last_week_end = (
        this_week_start - timedelta(days=1)
    )

    this_month_start = today.replace(day=1)

    last_month_end = (
        this_month_start - timedelta(days=1)
    )

    last_month_start = (
        last_month_end.replace(day=1)
    )

    last_month_same_end = (
        last_month_start + timedelta(
            days=(today.day - 1)
        )
    )

    return {

        "today": str(today),

        "this_week": {

            "start": str(this_week_start),
            "end": str(today)

        },

        "last_week": {

            "start": str(last_week_start),
            "end": str(last_week_end)

        },

        "this_month": {

            "start": str(this_month_start),
            "end": str(today)

        },

        "last_month_mtd": {

            "start": str(last_month_start),
            "end": str(last_month_same_end)

        }

    }

# =========================================================
# QUERY API
# =========================================================

@app.post("/query")
def query(payload: dict):

    sql = payload.get("sql")

    if not sql:

        return {

            "success": False,

            "error":
            "No SQL provided"

        }

    return run_athena_query(sql)

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    import uvicorn

    uvicorn.run(

        app,

        host="0.0.0.0",

        port=8000

    )
