"""
Vitess Test Helper - Connection utilities for VTGate
"""
import os, sys, string, inspect, time
import mysql.connector
from antithesis.random import random_choice, get_random
from antithesis.assertions import always, unreachable

def get_config():
    """Read configuration from environment variables."""
    return {
        'host': os.getenv('VTGATE_HOST', 'vtgate'),
        'port': int(os.getenv('VTGATE_MYSQL_PORT', '15306')),
        'keyspace': os.getenv('KEYSPACE', 'test_keyspace'),
    }

def generate_requests():
    return (get_random() % 100) + 1

def generate_random_string(length=16):
    random_str = []
    for _ in range(length):
        random_str.append(random_choice(list(string.ascii_letters + string.digits)))
    return "".join(random_str)

def get_connection(database=None):
    """
    Create a connection to VTGate.
    
    Args:
        database: Optional database/keyspace name. If None, connects without selecting a database.
    
    Returns:
        mysql.connector.connection.MySQLConnection
    """
    config = get_config()
    conn_params = {
        'host': config['host'],
        'port': config['port'],
        'database': config['keyspace'],
        'connection_timeout': 10,
        'autocommit': False,
    }
    if database:
        conn_params['database'] = database
    
    # Connect to VTGate
    try:
        conn = mysql.connector.connect(**conn_params)    
        log("Connected to VTGate")
        return conn
    except Exception as e:
        log(f"Connection to VTGate failed: {type(e).__name__}: {e}")
        raise

def get_connection_with_retry(database=None, max_retries=20, sleep_seconds=5, exit_on_failure=True):
    """
    Retry get_connection() up to max_retries times with sleep between attempts.

    Args:
        database: Optional database/keyspace name passed to get_connection().
        max_retries: Number of attempts before giving up.
        sleep_seconds: Seconds to sleep between attempts.
        exit_on_failure: If True, calls sys.exit(1) after exhausting retries.
                         If False, returns None.

    Returns:
        A mysql.connector connection, or None if exit_on_failure is False and all retries fail.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return get_connection(database=database)
        except Exception as e:
            log(f"Connection attempt {attempt}/{max_retries} failed: {type(e).__name__}: {e}")
            if attempt < max_retries:
                time.sleep(sleep_seconds)

    log(f"All {max_retries} connection attempts exhausted.")
    if exit_on_failure:
        sys.exit(1)
    return None


def retry_with_reconnect(operation, conn, max_retries=20, sleep_seconds=5):
    """
    Retry an operation with automatic reconnection on failure.

    Args:
        operation: Callable taking a connection, returns a tuple where
                   result[0] is a boolean success flag.
        conn: Current mysql.connector connection.
        max_retries: Number of retry attempts.
        sleep_seconds: Seconds to sleep between attempts.

    Returns:
        (result_tuple, conn) — conn may be a refreshed connection.
    """
    result = None
    last_exception = None
    for attempt in range(1, max_retries + 1):
        try:
            result = operation(conn)
            last_exception = None
            if result[0]:
                return result, conn
        except Exception as e:
            last_exception = e
            log(f"Operation raised exception on attempt {attempt}/{max_retries}: {type(e).__name__}: {e}")

        if result is not None:
            log(f"Operation failed on attempt {attempt}/{max_retries}: {result[1]}")
        if attempt < max_retries:
            time.sleep(sleep_seconds)
            if not conn.is_connected():
                log("Connection is dead, attempting reconnect...")
                new_conn = get_connection_with_retry(exit_on_failure=False, max_retries=3, sleep_seconds=sleep_seconds)
                if new_conn is not None:
                    conn = new_conn

    log(f"All {max_retries} retry attempts exhausted for operation.")
    if result is not None:
        return result, conn
    raise last_exception


def setup_test_table(conn, table_name='test'):
    """
    Create the test table if it doesn't exist.
    
    Args:
        conn: MySQL connection
        table_name: Name of the table to create
    
    Returns:
        True if successful
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGINT NOT NULL AUTO_INCREMENT,
                msg VARCHAR(128),
                PRIMARY KEY (id)
            ) ENGINE=InnoDB
        ''')
        cursor.close()
        conn.commit()
        return True, None
    except Exception as e:
        try:
            cursor.close()
        except Exception as close_err:
            log(f"cursor.close() failed: {close_err}")
        try:
            conn.rollback()
        except Exception as rollback_err:
            log(f"conn.rollback() failed: {rollback_err}")

        result_error = {
            "type": type(e).__name__,
            "message": str(e),
            "args": list(e.args)
        }

        return False, result_error

def insert_msg(conn, msg, table_name):
    """
    Insert a random message into table_name
    Returns (success, error, lastrowid)
    """
    row_id = get_random() % (2**53)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"INSERT INTO {table_name} (id, msg) VALUES (%s, %s)",
            (row_id, msg)
        )
        cursor.close()
        conn.commit()
        return True, None, row_id
    except Exception as e:
        try:
            cursor.close()
        except Exception as close_err:
            log(f"cursor.close() failed: {close_err}")
        try:
            conn.rollback()
        except Exception as rollback_err:
            log(f"conn.rollback() failed: {rollback_err}")

        result_error = {
            "type": type(e).__name__,
            "message": str(e),
            "args": list(e.args)
        }
        return False, result_error, None

def get_msg(conn, row_id, table_name, tablet_type='primary'):
    """
    Selects message based on row_id
    tablet_type: 'primary', 'replica', or 'rdonly' (default: 'primary')
    Returns (success, error, result)
    """
    config = get_config()
    cursor = conn.cursor()
    try:
        if tablet_type != 'primary':
            cursor.execute(f"USE {config['keyspace']}@{tablet_type}")
        else:
            cursor.execute(f"USE {config['keyspace']}")
        cursor.execute(
            f"SELECT id, msg FROM {table_name} WHERE id = %s",
            (row_id,)
        )
        result = cursor.fetchone()
        conn.commit()
        cursor.close()
        if result is not None:
            return True, None, result
        else: 
            return False, {"type": "NotFound", "message": f"No row found for id={row_id}"}, None

    except Exception as e:
        try:
            cursor.close()
        except Exception as close_err:
            log(f"cursor.close() failed: {close_err}")
            
        result_error = {
            "type": type(e).__name__,
            "message": str(e),
            "args": list(e.args)
        }

        return False, result_error, None

def update_msg(conn, row_id, new_msg, table_name):
    """
    Updates message for given row_id.
    Returns (success, error, rows_affected)
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"UPDATE {table_name} SET msg = %s WHERE id = %s",
            (new_msg, row_id)
        )
        rows_affected = cursor.rowcount
        cursor.close()
        conn.commit()
        return True, None, rows_affected
    except Exception as e:
        try:
            cursor.close()
        except Exception as close_err:
            log(f"cursor.close() failed: {close_err}")
        try:
            conn.rollback()
        except Exception as rollback_err:
            log(f"conn.rollback() failed: {rollback_err}")

        result_error = {
            "type": type(e).__name__,
            "message": str(e),
            "args": list(e.args)
        }

        return False, result_error, None

def delete_msg(conn, row_id, table_name):
    """
    Deletes row by row_id.
    Returns (success, error, rows_affected)
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"DELETE FROM {table_name} WHERE id = %s",
            (row_id,)
        )
        rows_affected = cursor.rowcount
        cursor.close()
        conn.commit()
        return True, None, rows_affected
    except Exception as e:
        try:
            cursor.close()
        except Exception as close_err:
            log(f"cursor.close() failed: {close_err}")
        try:
            conn.rollback()
        except Exception as rollback_err:
            log(f"conn.rollback() failed: {rollback_err}")

        result_error = {
            "type": type(e).__name__,
            "message": str(e),
            "args": list(e.args)
        }

        return False, result_error, None
    
def log(message):
    """Simple logging with prefix, pid, and caller filename."""
    pid = os.getpid()
    caller_frame = inspect.stack()[1]
    filename = os.path.basename(caller_frame.filename)
    print(f"[vitess-test] [pid:{pid}] [{filename}] {message}")