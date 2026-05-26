from fastapi import FastAPI
import boto3
import time
from datetime import date, timedelta
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

# =========================================================
# ATHENA CLIENT
# =========================================================

athena = boto3.client(
    "athena",
    region_name=REGION
)

# =========================================================
# ATHENA QUERY EXECUTOR
# =========================================================

def run_athena_query(query: str):

    print("\n================================================")
    print("EXECUTING ATHENA QUERY")
    print("================================================")
    print(query)

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

        query_execution_id = response["QueryExecutionId"]

        print(f"\nQueryExecutionId: {query_execution_id}")

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

                print(f"\nATHENA QUERY FAILED:\n{reason}")

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

            return {

                "success": True,
                "results": []

            }

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

        print(f"\nReturned {len(parsed_rows)} rows")

        return {

            "success": True,

            "query_execution_id":
            query_execution_id,

            "row_count":
            len(parsed_rows),

            "results":
            parsed_rows

        }

    except ClientError as e:

        print(f"\nAWS CLIENT ERROR:\n{str(e)}")

        return {

            "success": False,
            "error": str(e),
            "results": []

        }

    except Exception as e:

        print(f"\nUNEXPECTED ERROR:\n{str(e)}")

        return {

            "success": False,
            "error": str(e),
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
# HEALTH CHECK
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
# TABLE SCHEMA DISCOVERY
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
# ATHENA QUERY API
# =========================================================

@app.post("/query")
def query(payload: dict):

    sql = payload.get("sql")

    if not sql:

        return {

            "success": False,
            "error": "No SQL provided"

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
