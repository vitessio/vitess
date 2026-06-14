#!/usr/bin/env -S python3 -u

"""
Vitess Bank Test - First Setup

This script:
1. Creates accounts table (drops if exists)
2. Creates initial_state table (drops if exists)
3. Populates random accounts with random balances
4. Stores initial state for invariant checking
"""

import sys
from antithesis.random import get_random

sys.path.append("/opt/antithesis/resources")
import helper
from helper import log

def run_setup():
    config = helper.get_config()
    log(f"Starting bank test setup against {config['host']}:{config['port']}")
    log(f"Keyspace: {config['keyspace']}")
    
    # Connect to vtgate
    conn = helper.get_connection()
    cursor = conn.cursor()

    # Drop and create accounts table
    log("Creating accounts table...")
    try:
        cursor.execute("DROP TABLE IF EXISTS accounts")
        cursor.execute("""
            CREATE TABLE accounts (
                account_id BIGINT NOT NULL AUTO_INCREMENT,
                balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
                PRIMARY KEY (account_id)
            ) ENGINE=InnoDB
        """)
        conn.commit()
        log("accounts table created")
    except Exception as e:
        log(f"Failed to create accounts table: {e}")
        try:
            conn.rollback()
        except Exception as rollback_err:
            log(f"conn.rollback() failed: {rollback_err}")
        try:
            cursor.close()
        except Exception as close_err:
            log(f"cursor.close() failed: {close_err}")
        try:
            conn.close()
        except Exception as conn_err:
            log(f"conn.close() failed: {conn_err}")
        return False

    # Drop and create initial_state table
    log("Creating initial_state table...")
    try:
        cursor.execute("DROP TABLE IF EXISTS initial_state")
        cursor.execute("""
            CREATE TABLE initial_state (
                num_accts INT NOT NULL,
                total DECIMAL(15,2) NOT NULL
            ) ENGINE=InnoDB
        """)
        conn.commit()
        log("initial_state table created")
    except Exception as e:
        log(f"Failed to create initial_state table: {e}")
        try:
            conn.rollback()
        except Exception as rollback_err:
            log(f"conn.rollback() failed: {rollback_err}")
        try:
            cursor.close()
        except Exception as close_err:
            log(f"cursor.close() failed: {close_err}")
        try:
            conn.close()
        except Exception as conn_err:
            log(f"conn.close() failed: {conn_err}")
        return False

    # Generate random accounts with random balances
    num_accts = max(2, helper.generate_requests())
    total = 0
    log(f"Creating {num_accts} accounts...")

    for i in range(num_accts):
        account_id = get_random() % (2**53)
        balance = get_random() % int(1e9)
        total += balance
        try:
            cursor.execute(
                "INSERT INTO accounts (account_id, balance) VALUES (%s, %s)",
                (account_id, balance)
            )
        except Exception as e:
            log(f"Failed to insert account {i}: {e}")
            try:
                conn.rollback()
            except Exception as rollback_err:
                log(f"conn.rollback() failed: {rollback_err}")
            try:
                cursor.close()
            except Exception as close_err:
                log(f"cursor.close() failed: {close_err}")
            try:
                conn.close()
            except Exception as conn_err:
                log(f"conn.close() failed: {conn_err}")
            return False

    try:
        conn.commit()
    except Exception as e:
        log(f"Failed to commit account inserts: {e}")
        try:
            conn.rollback()
        except Exception as rollback_err:
            log(f"conn.rollback() failed: {rollback_err}")
        try:
            cursor.close()
        except Exception as close_err:
            log(f"cursor.close() failed: {close_err}")
        try:
            conn.close()
        except Exception as conn_err:
            log(f"conn.close() failed: {conn_err}")
        return False

    log(f"Created {num_accts} accounts with total balance: {total}")

    # Store initial state
    log("Storing initial state...")
    try:
        cursor.execute(
            "INSERT INTO initial_state (num_accts, total) VALUES (%s, %s)",
            (num_accts, total)
        )
        conn.commit()
        log(f"Initial state stored: num_accts={num_accts}, total={total}")
    except Exception as e:
        log(f"Failed to store initial state: {e}")
        try:
            conn.rollback()
        except Exception as rollback_err:
            log(f"conn.rollback() failed: {rollback_err}")
        try:
            cursor.close()
        except Exception as close_err:
            log(f"cursor.close() failed: {close_err}")
        try:
            conn.close()
        except Exception as conn_err:
            log(f"conn.close() failed: {conn_err}")
        return False

    try:
        cursor.close()
    except Exception as close_err:
        log(f"cursor.close() failed: {close_err}")
    try:
        conn.close()
    except Exception as conn_err:
        log(f"conn.close() failed: {conn_err}")
    
    log("Connection closed")
    log("BANK TEST SETUP COMPLETE")
    return True


if __name__ == '__main__':
    success = run_setup()
    sys.exit(0 if success else 1)