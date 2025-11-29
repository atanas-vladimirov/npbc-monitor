# -*- coding: utf-8 -*-

import os
import sys
import time
import logging
import logging.handlers
import argparse
import uvicorn
from datetime import datetime
from typing import Optional

# --- FastAPI Imports ---
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# --- Database Imports ---
import psycopg2
import psycopg2.extras

# --- Configuration ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = '/var/log/npbc_monitor.log'

# --- Pydantic Models ---
class BurnerLogSchema(BaseModel):
    SwVer: str
    Date: str
    Mode: int
    State: int
    Status: int
    IgnitionFail: bool
    PelletJam: bool
    Tset: int
    Tboiler: int
    Flame: int
    Heater: bool
    DHW: int
    DHWPump: bool
    CHPump: bool
    BF: bool
    FF: bool
    Fan: int
    Power: int
    ThermostatStop: bool
    FFWorkTime: int
    TDS18: float
    TBMP: float
    PBMP: float
    KTYPE: float

# --- Logging Setup ---

class SafeIPFormatter(logging.Formatter):
    """
    Ensures 'client_ip' exists in every log record.
    Defaults to '-' for system messages.
    """
    def format(self, record):
        if not hasattr(record, 'client_ip'):
            record.client_ip = "-"
        return super().format(record)

def setup_logging():
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Format: Date Time - IP - Level - Message
    log_fmt = '%(asctime)s - %(client_ip)s - %(levelname)s - %(message)s'
    date_fmt = '%Y-%m-%d %H:%M:%S'

    formatter = SafeIPFormatter(log_fmt, datefmt=date_fmt)
    file_handler = logging.handlers.WatchedFileHandler(LOG_FILE)
    file_handler.setFormatter(formatter)

    # Clear existing handlers to avoid duplication
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    root_logger.addHandler(file_handler)

# --- Database Utilities ---

def get_db_connection(args):
    try:
        conn = psycopg2.connect(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password
        )
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to DB: {e}")
        sys.exit(1)

def initialize_database(args):
    conn = get_db_connection(args)
    try:
        with conn.cursor() as cur:
            # 1. Create the main log table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS "BurnerLogs" (
                    "Timestamp" TIMESTAMP NOT NULL PRIMARY KEY,
                    "SwVer" VARCHAR(50) NOT NULL,
                    "Date" TIMESTAMP NOT NULL,
                    "Mode" INTEGER NOT NULL,
                    "State" INTEGER NOT NULL,
                    "Status" INTEGER NOT NULL,
                    "IgnitionFail" BOOLEAN NOT NULL,
                    "PelletJam" BOOLEAN NOT NULL,
                    "Tset" INTEGER NOT NULL,
                    "Tboiler" INTEGER NOT NULL,
                    "Flame" INTEGER NOT NULL,
                    "Heater" BOOLEAN NOT NULL,
                    "DHW" INTEGER NOT NULL,
                    "DHWPump" BOOLEAN NOT NULL,
                    "CHPump" BOOLEAN NOT NULL,
                    "BF" BOOLEAN NOT NULL,
                    "FF" BOOLEAN NOT NULL,
                    "Fan" INTEGER NOT NULL,
                    "Power" INTEGER NOT NULL,
                    "ThermostatStop" BOOLEAN NOT NULL,
                    "FFWorkTime" INTEGER NOT NULL,
                    "TDS18" REAL NOT NULL,
                    "TBMP" REAL NOT NULL,
                    "PBMP" REAL NOT NULL,
                    "KTYPE" REAL NOT NULL
                )
            """)

            # 2. Create the summary cache table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS "MonthlyStats" (
                    "Month" DATE NOT NULL PRIMARY KEY,
                    "FFWorkTime" INTEGER NOT NULL DEFAULT 0
                )
            """)

            # 3. Seed/Backfill the cache
            logging.info("Seeding MonthlyStats cache from existing logs...")
            cur.execute("""
                INSERT INTO "MonthlyStats" ("Month", "FFWorkTime")
                SELECT date_trunc('month', "Timestamp")::date, SUM("FFWorkTime")
                FROM "BurnerLogs"
                WHERE "Timestamp" < date_trunc('month', NOW())
                GROUP BY 1
                ON CONFLICT ("Month") DO NOTHING
            """)

            conn.commit()
            logging.info("Database initialized.")

    except Exception as e:
        logging.error(f"DB Init failed: {e}")
        conn.rollback()
    finally:
        conn.close()

# Call this at startup or periodically
def ensure_monthly_stats_up_to_date(conn_args):
    """
    Checks if the previous completed month exists in MonthlyStats.
    If not, it calculates it from BurnerLogs and inserts it.
    """
    conn = get_db_connection(conn_args)
    try:
        with conn.cursor() as cur:
            # 1. Determine the start of the current month and the previous month
            cur.execute("SELECT date_trunc('month', NOW())::date, date_trunc('month', NOW() - INTERVAL '1 month')::date")
            current_month_start, prev_month_start = cur.fetchone()

            # 2. Check if previous month exists in Summary
            cur.execute('SELECT 1 FROM "MonthlyStats" WHERE "Month" = %s', (prev_month_start,))
            if cur.fetchone() is None:
                logging.info(f"Caching stats for completed month: {prev_month_start}")

                # Calculate and Insert (Atomic operation)
                cur.execute("""
                    INSERT INTO "MonthlyStats" ("Month", "FFWorkTime")
                    SELECT date_trunc('month', "Timestamp")::date, SUM("FFWorkTime")
                    FROM "BurnerLogs"
                    WHERE "Timestamp" >= %s AND "Timestamp" < %s
                    GROUP BY 1
                    ON CONFLICT ("Month") DO NOTHING
                """, (prev_month_start, current_month_start))

                conn.commit()
            else:
                logging.debug("Monthly stats are up to date.")
    except Exception as e:
        logging.error(f"Failed to update monthly stats: {e}")
    finally:
        conn.close()

# --- FastAPI App Setup ---

app = FastAPI(docs_url=None, redoc_url=None)
app_args = None

# --- Middleware: The Single Logger ---
@app.middleware("http")
async def custom_logging_middleware(request: Request, call_next):
    start_time = time.time()

    response = await call_next(request)

    process_time = (time.time() - start_time) * 1000

    # Get IP (Handling Nginx Proxy Headers)
    client_ip = request.headers.get("x-real-ip") or request.client.host

    # Log Format: "HTTP_Status Method URL Duration"
    # Example: 200 POST /api/logData 43.11ms
    msg = "%d %s %s %.2fms" % (
        response.status_code,
        request.method,
        request.url.path,
        process_time
    )

    if response.status_code < 400:
        level = logging.INFO
    elif response.status_code < 500:
        level = logging.WARNING
    else:
        level = logging.ERROR

    logging.getLogger().log(level, msg, extra={'client_ip': client_ip})

    return response

# --- API Routes ---

@app.post("/api/logData")
def log_data(data: BurnerLogSchema):
    try:
        conn = get_db_connection(app_args)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO "BurnerLogs" (
                    "Timestamp", "SwVer", "Date", "Mode", "State", "Status", "IgnitionFail", "PelletJam",
                    "Tset", "Tboiler", "Flame", "Heater", "DHWPump", "CHPump", "DHW", "BF", "FF", "Fan",
                    "Power", "ThermostatStop", "FFWorkTime", "TDS18", "TBMP", "PBMP", "KTYPE"
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                datetime.now(), data.SwVer, data.Date, data.Mode, data.State, data.Status,
                data.IgnitionFail, data.PelletJam, data.Tset, data.Tboiler,
                data.Flame, data.Heater, data.DHWPump, data.CHPump,
                data.DHW, data.BF, data.FF, data.Fan, data.Power,
                data.ThermostatStop, data.FFWorkTime, data.TDS18, data.TBMP,
                data.PBMP, data.KTYPE
            ))
            conn.commit()
        conn.close()
        return {"message": "OK"}
    except Exception as e:
        logging.error(f"Error logging data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/getInfo")
def get_info():
    conn = get_db_connection(app_args)
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT "SwVer", "Power", "Flame", "Tset", "Tboiler", "State", "Status", "DHW", "Fan", "DHWPump", "CHPump", "Mode", "TBMP"
            FROM "BurnerLogs" WHERE "Timestamp" >= NOW() - INTERVAL '1 minute'
            ORDER BY "Date" DESC LIMIT 1
        """)
        result = [dict(row) for row in cur.fetchall()]
    conn.close()
    return result

@app.get("/api/getStats")
def get_stats(timestamp: Optional[str] = None, limit: int = 7000, page: int = 1):
    if limit > 10000: limit = 10000
    offset = (page - 1) * limit

    query = """
        SELECT to_char("Timestamp", 'YYYY-MM-DD"T"HH24:MI:SS') AS "Date",
               "Power", "Flame", "Tset", "Tboiler", "DHW", "ThermostatStop", "TDS18", "KTYPE", "TBMP"
        FROM "BurnerLogs"
    """
    params = []
    where_clauses = []

    if timestamp and timestamp != "null":
        try:
            ts = datetime.fromtimestamp(float(timestamp))
            where_clauses.append('"Date" >= %s')
            params.append(ts)
        except ValueError:
             raise HTTPException(status_code=400, detail="Invalid timestamp")
    else:
        where_clauses.append("\"Date\" >= NOW() - INTERVAL '24 hours'")

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += ' ORDER BY "Date" ASC LIMIT %s OFFSET %s'
    params.extend([limit, offset])

    conn = get_db_connection(app_args)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, params)
            result = [dict(row) for row in cur.fetchall()]
        return result
    finally:
        conn.close()

@app.get("/api/getConsumptionByMonth")
def get_consumption_by_month():
    # 1. First, ensure our cache is up to date (lazy check)
    ensure_monthly_stats_up_to_date(app_args)

    conn = get_db_connection(app_args)
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Combines the fast "MonthlyStats" table with a small query on "BurnerLogs"
        # for ONLY the current month.
        query = """
            SELECT to_char("Month", 'YYYY-MM') AS yr_mon, "FFWorkTime" as "FFWork"
            FROM "MonthlyStats"

            UNION ALL

            SELECT to_char(date_trunc('month', "Timestamp"), 'YYYY-MM') AS yr_mon, SUM("FFWorkTime") as "FFWork"
            FROM "BurnerLogs"
            WHERE "Timestamp" >= date_trunc('month', NOW())
            GROUP BY date_trunc('month', "Timestamp")

            ORDER BY yr_mon;
        """
        cur.execute(query)
        result = [dict(row) for row in cur.fetchall()]
    conn.close()
    return result

@app.get("/api/getConsumptionStats")
def get_consumption_stats(timestamp: Optional[str] = None):
    try:
        if timestamp:
            timestamp_sec = int(timestamp)
        else:
             timestamp_sec = int((datetime.now().timestamp()) - (24 * 3600))
    except ValueError:
         timestamp_sec = int((datetime.now().timestamp()) - (24 * 3600))

    start_time = datetime.fromtimestamp(timestamp_sec)

    conn = get_db_connection(app_args)
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT
                to_char(hour_bucket, 'YYYY-MM-DD"T"HH24:MI:SS') as "Timestamp",
                COALESCE(SUM(bl."FFWorkTime"), 0) as "FFWorkTime"
            FROM
                generate_series(
                    date_trunc('hour', %s::timestamp),
                    date_trunc('hour', NOW()),
                    '1 hour'
                ) AS hour_bucket
            LEFT JOIN "BurnerLogs" AS bl
                ON bl."Timestamp" >= hour_bucket AND bl."Timestamp" < hour_bucket + interval '1 hour'
            GROUP BY hour_bucket
            ORDER BY hour_bucket;
        """, (start_time,))
        result = [dict(row) for row in cur.fetchall()]
    conn.close()
    return result

# --- Static Files & Routing ---

# 1. Mount static folder
static_dir = os.path.join(BASE_DIR, "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/")
async def serve_root():
    index_path = os.path.join(BASE_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return Response("Index not found", status_code=404)

@app.get("/{file_name}")
async def serve_root_files(file_name: str):
    allowed = ["favicon.ico", "manifest.json", "robots.txt", "asset-manifest.json"]

    if file_name in allowed or (file_name.startswith("logo") and file_name.endswith(".png")):
        file_path = os.path.join(BASE_DIR, file_name)
        if os.path.exists(file_path):
            # Return correct media type for favicon
            media_type = "image/x-icon" if file_name == "favicon.ico" else None
            return FileResponse(file_path, media_type=media_type)

    index_path = os.path.join(BASE_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)

    return Response("Not Found", status_code=404)

# --- Main Entry Point ---

def parse_arguments():
    parser = argparse.ArgumentParser(description="NPBC Monitor Server")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8088, help="Run on the given port")
    parser.add_argument("--db_host", default="localhost")
    parser.add_argument("--db_port", default=5432)
    parser.add_argument("--db_name", default="npbc_db")
    parser.add_argument("--db_user", default="npbc_user")
    parser.add_argument("--db_password", default=None)
    parser.add_argument("--init_db", type=bool, default=False)
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    app_args = args

    setup_logging()

    if args.init_db:
        initialize_database(args)
        sys.exit(0)

    logging.info(f"Script running from: {BASE_DIR}")
    logging.info(f"Starting FastAPI on port {args.port}...")

    # We rely entirely on our custom middleware for request logging.
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_config=None,
        access_log=False,
        forwarded_allow_ips="*"
    )
