# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import logging
from datetime import datetime

# --- Tornado Imports ---
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.escape
from tornado.options import define, options

# --- Database Imports ---
import psycopg2
import psycopg2.extras

# --- Configuration ---
LOG_FILE = '/var/log/npbc_monitor.log'  # Define the log file path

# Set up file logging
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Get the root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Clear existing handlers (like the default StreamHandler to console)
if root_logger.hasHandlers():
    root_logger.handlers.clear()

# Add the file handler
root_logger.addHandler(file_handler)

logging.info("Logging initialized to file.")
# Optionally, if you want Tornado messages to go to the file as well:
logging.getLogger('tornado.access').addHandler(file_handler)
logging.getLogger('tornado.application').addHandler(file_handler)

# Define command line options for server and database configuration
define("port", default=8088, help="run on the given port", type=int)
define("db_host", default="localhost", help="PostgreSQL database host")
define("db_port", default=5432, help="PostgreSQL database port")
define("db_name", default="npbc_db", help="PostgreSQL database name")
define("db_user", default="npbc_user", help="PostgreSQL database user")
define("db_password", default="mTKgIi0HCCTiUKF", help="PostgreSQL database password")
define("init_db", default=False, help="Initialize the database schema", type=bool)

# --- Database Utilities ---

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=options.db_host,
            port=options.db_port,
            dbname=options.db_name,
            user=options.db_user,
            password=options.db_password
        )
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Could not connect to PostgreSQL database: {e}")
        sys.exit(1)

def initialize_database():
    """Creates the BurnerLogs table in the PostgreSQL database if it doesn't exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
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
            conn.commit()
            logging.info("Database initialized successfully. 'BurnerLogs' table is ready.")
    except Exception as e:
        logging.error(f"Database initialization failed: {e}")
        conn.rollback()
    finally:
        conn.close()

# --- Tornado Request Handlers ---

class BaseHandler(tornado.web.RequestHandler):
    """Base handler to set common headers."""
    def set_default_headers(self):
        self.set_header("Content-Type", 'application/json; charset="utf-8"')
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "content-type")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def options(self, *args):
        self.set_status(204)
        self.finish()

class LogDataHandler(BaseHandler):
    """Handles incoming log data, replacing the old server.py."""
    def post(self):
        try:
            data = tornado.escape.json_decode(self.request.body)
            #print("--- The data ---")
            #print(data)

            # Map TINYINT (0/1) from original data to BOOLEAN for PostgreSQL
            params = [
                datetime.now(), data["SwVer"], data["Date"], data["Mode"], data["State"], data["Status"],
                bool(data["IgnitionFail"]), bool(data["PelletJam"]), data["Tset"], data["Tboiler"],
                data["Flame"], bool(data["Heater"]), bool(data["DHWPump"]), bool(data["CHPump"]),
                data["DHW"], bool(data["BF"]), bool(data["FF"]), data["Fan"], data["Power"],
                bool(data["ThermostatStop"]), data["FFWorkTime"], data["TDS18"], data["TBMP"],
                data["PBMP"], data["KTYPE"]
            ]
            #print("--- The params --- ")
            #print(params)

            query = """
                INSERT INTO "BurnerLogs" (
                    "Timestamp", "SwVer", "Date", "Mode", "State", "Status", "IgnitionFail", "PelletJam",
                    "Tset", "Tboiler", "Flame", "Heater", "DHWPump", "CHPump", "DHW", "BF", "FF", "Fan",
                    "Power", "ThermostatStop", "FFWorkTime", "TDS18", "TBMP", "PBMP", "KTYPE"
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()

            self.set_status(200)
            self.write({"message": "Log received successfully."})

        except (json.JSONDecodeError, KeyError) as e:
            self.set_status(400)
            self.write({"error": f"Invalid or missing data in request: {e}"})
        except Exception as e:
            logging.error(f"Error logging data: {e}")
            self.set_status(500)
            self.write({"error": "Internal server error while logging data."})
        finally:
            if 'conn' in locals() and conn:
                conn.close()

class GetInfoHandler(BaseHandler):
    def get(self):
        query = """
            SELECT "SwVer", "Power", "Flame", "Tset", "Tboiler", "State", "Status", "DHW", "Fan", "DHWPump", "CHPump", "Mode", "TBMP"
            FROM "BurnerLogs" WHERE "Timestamp" >= NOW() - INTERVAL '1 minute'
            ORDER BY "Date" DESC LIMIT 1
        """
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query)
            result = [dict(row) for row in cur.fetchall()]
        conn.close()
        self.write(json.dumps(result, default=str))

class GetStatsHandler(BaseHandler):
    def get(self):
        timestamp_arg = self.get_argument('timestamp', None)
        try:
            limit = int(self.get_argument('limit', '7000'))
            page = int(self.get_argument('page', '1'))
        except ValueError:
            self.set_status(400)
            self.write({"error": "Invalid 'limit' or 'page' parameter. Must be an integer."})
            return

        offset = (page - 1) * limit

        query = """
            SELECT to_char("Timestamp", 'YYYY-MM-DD"T"HH24:MI:SS') AS "Date",
                   "Power", "Flame", "Tset", "Tboiler", "DHW", "ThermostatStop", "TDS18", "KTYPE", "TBMP"
            FROM "BurnerLogs"
        """
        params = []
        where_clauses = []

        if timestamp_arg and timestamp_arg != "null":
            try:
                # FIX: Convert Unix timestamp (seconds) to datetime object
                ts = datetime.fromtimestamp(float(timestamp_arg))
                where_clauses.append('"Date" >= %s')
                params.append(ts)
            except ValueError:
                 self.set_status(400)
                 self.write({"error": "Invalid timestamp format."})
                 return
        else:
            where_clauses.append("\"Date\" >= NOW() - INTERVAL '24 hours'")

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        # FIX: Add pagination to the query to prevent MemoryError
        query += ' ORDER BY "Date" ASC LIMIT %s OFFSET %s'
        params.extend([limit, offset])

        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params)
                # fetchall() is now safe because of the LIMIT clause
                result = [dict(row) for row in cur.fetchall()]
            self.write(json.dumps(result, default=str))
        except Exception as e:
            logging.error(f"Error fetching stats: {e}")
            self.set_status(500)
            self.write({"error": "Internal server error while fetching stats."})
        finally:
            if conn:
                conn.close()


class GetConsumptionByMonthHandler(BaseHandler):
    def get(self):

        query = """
            SELECT to_char(date_trunc('month', "Timestamp"), 'YYYY-MM') AS yr_mon, SUM("FFWorkTime") as "FFWork"
            FROM "BurnerLogs"
            WHERE "Timestamp" >= NOW() - INTERVAL '1 year'
            GROUP BY date_trunc('month', "Timestamp")
            ORDER BY date_trunc('month', "Timestamp");
        """
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query)
            result = [dict(row) for row in cur.fetchall()]
        conn.close()

        self.write(json.dumps(result, default=str))


class GetConsumptionStatsHandler(BaseHandler):
    def get(self):
        try:
            # Get the 'timestamp' argument from the URL (e.g., ?timestamp=1678886400)
            timestamp_sec = int(self.get_argument('timestamp'))
        except (ValueError, TypeError, tornado.web.MissingArgumentError):
            # Fallback to 24 hours ago if the parameter is missing or invalid
            timestamp_sec = int((datetime.now() - datetime.timedelta(hours=24)).timestamp())

        start_time = datetime.fromtimestamp(timestamp_sec)

        # We replace the hard-coded '24 hours' with a placeholder (%s)
        query = """
            SELECT
                to_char(hour_bucket, 'YYYY-MM-DD"T"HH24:MI:SS') as "Timestamp",
                COALESCE(SUM(bl."FFWorkTime"), 0) as "FFWorkTime"
            FROM
                generate_series(
                    date_trunc('hour', %s::timestamp),  -- <-- CHANGED: Use the placeholder
                    date_trunc('hour', NOW()),
                    '1 hour'
                ) AS hour_bucket
            LEFT JOIN "BurnerLogs" AS bl
                ON bl."Timestamp" >= hour_bucket AND bl."Timestamp" < hour_bucket + interval '1 hour'
            GROUP BY hour_bucket
            ORDER BY hour_bucket;
        """

        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, (start_time,))
            result = [dict(row) for row in cur.fetchall()]
        conn.close()
        self.write(json.dumps(result, default=str))

def make_app():
    """Creates the Tornado web application and defines URL routing."""
    REACT_BUILD_PATH = os.path.join(os.path.dirname(__file__))
    return tornado.web.Application(
        [
            (r"/api/logData", LogDataHandler), # New endpoint for logging
            (r"/api/getInfo", GetInfoHandler),
            (r"/api/getStats", GetStatsHandler),
            (r"/api/getConsumptionStats", GetConsumptionStatsHandler),
            (r"/api/getConsumptionByMonth", GetConsumptionByMonthHandler),
            (r"/(.*)", tornado.web.StaticFileHandler, {
                "path": REACT_BUILD_PATH,
                "default_filename": "index.html"
            }),
        ],
        static_path=os.path.join(REACT_BUILD_PATH, "static"),
        debug=True,
    )

def main():
    """Main function to parse arguments and start the server."""
    tornado.options.parse_command_line()

    if options.init_db:
        initialize_database()
        return

    # Test DB connection on startup
    logging.info("Attempting to connect to the database...")
    conn = get_db_connection()
    conn.close()
    logging.info("Database connection successful.")

    app = make_app()
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)

    logging.info(f"Server is running on http://localhost:{options.port}")
    logging.info("To initialize the database, run with --init_db=true")
    logging.info("Press Ctrl+C to stop the server.")

    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()
