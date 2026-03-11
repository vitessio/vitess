"""
Vitess Test Helper - Connection utilities for VTGate
"""
import os, sys, string, inspect
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
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"INSERT INTO {table_name} (msg) VALUES (%s)",
            (msg,)
        )
        lastrowid = cursor.lastrowid
        cursor.close()
        conn.commit()
        return True, None, lastrowid
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
        cursor.execute(
            f"SELECT id, msg FROM {table_name} WHERE id = %s",
            (row_id,)
        )
        result = cursor.fetchone()
        cursor.close()
        return True, None, result
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