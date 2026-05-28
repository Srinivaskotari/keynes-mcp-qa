from fastapi import FastAPI
import boto3
import time
import os
import json
import uuid
import threading
import redis

from datetime import datetime

from datasets import DATASETS

# =========================================================
# FASTAPI APP
# =========================================================

app = FastAPI()

# =========================================================
# CONFIG
# =========================================================

REGION = "us-east-1"

DATABASE = "domo_reports_s3"

OUTPUT_BUCKET = "s3://keynes-athena-results-srinivas"

MAX_QUERY_LENGTH = 5000

CACHE_TTL_SECONDS = 300

LOG_DIR = "logs"

LOG_FILE = f"{LOG_DIR}/query_history.log"

# =========================================================
# REDIS CLIENT
# =========================================================

redis_client = redis.Redis(

    host="localhost",

    port=6379,

    decode_responses=True

)

# =========================================================
# SECURITY
# =========================================================

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
# LOG DIRECTORY
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
# LOGGER
# =========================================================

def log_query(entry: dict):

    with open(LOG_FILE, "a") as f:

        f.write(json.dumps(entry) + "\n")

# =========================================================
# SQL VALIDATION
# =========================================================

def validate_sql(sql: str):

    sql_upper = sql.upper()

    if len(sql) > MAX_QUERY_LENGTH:

        return {

            "valid": False,

            "error":
            "Query exceeds maximum allowed length"

        }

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

    for keyword in BLOCKED_KEYWORDS:

        if keyword in sql_upper:

            return {

                "valid": False,

                "error":
                f"Blocked keyword detected: {keyword}"

            }

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
# REDIS HELPERS
# =========================================================

def get_cache_key(query):

    return f"cache:{query}"

def get_query_key(query_id):

    return f"query:{query_id}"

# =========================================================
# BACKGROUND QUERY EXECUTION
# =========================================================

def execute_query_background(query_id, query):

    start_time = time.time()

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

        athena_execution_id = response[
            "QueryExecutionId"
        ]

        redis_client.set(

            get_query_key(query_id),

            json.dumps({

                "query": query,

                "status": "RUNNING",

                "athena_execution_id":
                athena_execution_id,

                "start_time":
                str(datetime.utcnow())

            })

        )

        while True:

            status = athena.get_query_execution(

                QueryExecutionId=athena_execution_id

            )

            state = status[
                "QueryExecution"
            ]["Status"]["State"]

            current_data = json.loads(

                redis_client.get(

                    get_query_key(query_id)

                )

            )

            current_data["status"] = state

            redis_client.set(

                get_query_key(query_id),

                json.dumps(current_data)

            )

            if state == "SUCCEEDED":
                break

            if state in ["FAILED", "CANCELLED"]:

                current_data["status"] = state

                redis_client.set(

                    get_query_key(query_id),

                    json.dumps(current_data)

                )

                return

            time.sleep(2)

        results = athena.get_query_results(

            QueryExecutionId=athena_execution_id

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

        current_data = json.loads(

            redis_client.get(

                get_query_key(query_id)

            )

        )

        current_data["status"] = "COMPLETED"

        current_data[
            "execution_time_seconds"
        ] = execution_time

        redis_client.set(

            get_query_key(query_id),

            json.dumps(current_data)

        )

        redis_client.setex(

            get_cache_key(query),

            CACHE_TTL_SECONDS,

            json.dumps(parsed_rows)

        )

        redis_client.set(

            f"result:{query_id}",

            json.dumps(parsed_rows)

        )

        log_query({

            "timestamp":
            str(datetime.utcnow()),

            "query_id":
            query_id,

            "query":
            query,

            "execution_time_seconds":
            execution_time,

            "row_count":
            len(parsed_rows)

        })

    except Exception as e:

        redis_client.set(

            get_query_key(query_id),

            json.dumps({

                "status": "FAILED",

                "error": str(e)

            })

        )

# =========================================================
# HOME
# =========================================================

@app.get("/")
def home():

    return {

        "message":
        "Keynes QA Analytics API Running"

    }

# =========================================================
# HEALTH
# =========================================================

@app.get("/health")
def health():

    return {

        "status":
        "healthy"

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
# ACTIVE QUERIES
# =========================================================

@app.get("/active-queries")
def active_queries():

    keys = redis_client.keys("query:*")

    queries = {}

    for key in keys:

        queries[key] = json.loads(

            redis_client.get(key)

        )

    return queries

# =========================================================
# QUERY HISTORY
# =========================================================

@app.get("/query-history")
def query_history():

    try:

        with open(LOG_FILE, "r") as f:

            lines = f.readlines()

        return {

            "queries":
            [json.loads(x) for x in lines[-20:]]

        }

    except:

        return {

            "queries": []

        }

# =========================================================
# CACHE STATS
# =========================================================

@app.get("/cache-stats")
def cache_stats():

    cache_keys = redis_client.keys("cache:*")

    result_keys = redis_client.keys("result:*")

    query_keys = redis_client.keys("query:*")

    return {

        "cache_entries":
        len(cache_keys),

        "stored_results":
        len(result_keys),

        "tracked_queries":
        len(query_keys)

    }

# =========================================================
# SYSTEM HEALTH
# =========================================================

@app.get("/system-health")
def system_health():

    redis_status = "healthy"

    try:

        redis_client.ping()

    except:

        redis_status = "unhealthy"

    return {

        "api": "healthy",

        "redis": redis_status,

        "athena_region": REGION,

        "database": DATABASE

    }

# =========================================================
# QUERY SUMMARY
# =========================================================

@app.get("/query-summary")
def query_summary():

    try:

        with open(LOG_FILE, "r") as f:

            lines = f.readlines()

        total_queries = len(lines)

        total_execution_time = 0

        successful_queries = 0

        for line in lines:

            try:

                entry = json.loads(line)

                execution_time = entry.get(

                    "execution_time_seconds",

                    0

                )

                total_execution_time += execution_time

                successful_queries += 1

            except:

                pass

        average_latency = 0

        if successful_queries > 0:

            average_latency = round(

                total_execution_time /
                successful_queries,

                2

            )

        return {

            "total_queries":
            total_queries,

            "successful_queries":
            successful_queries,

            "average_latency_seconds":
            average_latency

        }

    except Exception as e:

        return {

            "error": str(e)

        }

# =========================================================
# SLOW QUERIES
# =========================================================

@app.get("/slow-queries")
def slow_queries():

    slow = []

    try:

        with open(LOG_FILE, "r") as f:

            lines = f.readlines()

        for line in lines:

            try:

                entry = json.loads(line)

                execution_time = entry.get(

                    "execution_time_seconds",

                    0

                )

                if execution_time >= 5:

                    slow.append(entry)

            except:

                pass

        slow = sorted(

            slow,

            key=lambda x:
            x.get(
                "execution_time_seconds",
                0
            ),

            reverse=True

        )

        return {

            "slow_queries":
            slow[:20]

        }

    except Exception as e:

        return {

            "error": str(e)

        }

# =========================================================
# METRICS
# =========================================================

@app.get("/metrics")
def metrics():

    try:

        cache_keys = redis_client.keys("cache:*")

        result_keys = redis_client.keys("result:*")

        query_keys = redis_client.keys("query:*")

        with open(LOG_FILE, "r") as f:

            lines = f.readlines()

        total_queries = len(lines)

        total_execution_time = 0

        for line in lines:

            try:

                entry = json.loads(line)

                total_execution_time += entry.get(

                    "execution_time_seconds",

                    0

                )

            except:

                pass

        average_latency = 0

        if total_queries > 0:

            average_latency = round(

                total_execution_time /
                total_queries,

                2

            )

        return {

            "total_queries":
            total_queries,

            "average_latency_seconds":
            average_latency,

            "cache_entries":
            len(cache_keys),

            "stored_results":
            len(result_keys),

            "tracked_queries":
            len(query_keys),

            "redis_status":
            "healthy"

        }

    except Exception as e:

        return {

            "error": str(e)

        }

# =========================================================
# START QUERY
# =========================================================

@app.post("/query/start")
def start_query(payload: dict):

    sql = payload.get("sql")

    if not sql:

        return {

            "success": False,

            "error":
            "No SQL provided"

        }

    validation = validate_sql(sql)

    if not validation["valid"]:

        return {

            "success": False,

            "error":
            validation["error"]

        }

    sql = validation["sql"]

    cache_key = get_cache_key(sql)

    cached = redis_client.get(cache_key)

    if cached:

        return {

            "success": True,

            "cached": True,

            "results":
            json.loads(cached)

        }

    query_id = str(uuid.uuid4())

    redis_client.set(

        get_query_key(query_id),

        json.dumps({

            "query": sql,

            "status": "STARTING",

            "start_time":
            str(datetime.utcnow())

        })

    )

    thread = threading.Thread(

        target=execute_query_background,

        args=(query_id, sql)

    )

    thread.start()

    return {

        "success": True,

        "query_id": query_id,

        "status": "STARTED"

    }

# =========================================================
# QUERY STATUS
# =========================================================

@app.get("/query/status/{query_id}")
def query_status(query_id: str):

    data = redis_client.get(

        get_query_key(query_id)

    )

    if not data:

        return {

            "success": False,

            "error":
            "Query not found"

        }

    return json.loads(data)

# =========================================================
# QUERY RESULT
# =========================================================

@app.get("/query/result/{query_id}")
def query_result(query_id: str):

    result = redis_client.get(

        f"result:{query_id}"

    )

    if not result:

        return {

            "success": False,

            "message":
            "Result not ready"

        }

    return {

        "success": True,

        "results":
        json.loads(result)

    }

# =========================================================
# LEGACY QUERY
# =========================================================

@app.post("/query")
def query(payload: dict):

    return start_query(payload)

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
