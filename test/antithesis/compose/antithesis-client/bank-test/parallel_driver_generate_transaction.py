#!/usr/bin/env -S python3 -u

"""
Vitess Bank Test - Transaction Generator

This script:
1. Connects to VTGate
2. Runs random transfers between accounts
3. Each transfer is atomic (BEGIN/COMMIT/ROLLBACK)
"""

import sys
from antithesis.random import get_random

sys.path.append("/opt/antithesis/resources")
import helper
from helper import log


def get_account_ids(conn):
    """Get list of actual account IDs from accounts table."""
    cursor = conn.cursor()
    account_ids = []
    try:
        cursor.execute("SELECT account_id FROM accounts")
        rows = cursor.fetchall()
        account_ids = [row[0] for row in rows]
    except Exception as e:
        log(f"failed to get account IDs: {e}")
    finally:
        try:
            cursor.close()
        except Exception as close_err:
            log(f"cursor.close() failed: {close_err}")
    return account_ids


def transfer(conn, sender, recipient, value):
    """
    Transfer value from sender to recipient.
    Returns (success, error)
    """
    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN")
        cursor.execute(
            "UPDATE accounts SET balance = balance - %s WHERE account_id = %s",
            (value, sender)
        )
        cursor.execute(
            "UPDATE accounts SET balance = balance + %s WHERE account_id = %s",
            (value, recipient)
        )
        cursor.execute("COMMIT")
        cursor.close()
        return True, None
    except Exception as e:
        # Try to rollback, but don't fail if rollback also fails
        try:
            cursor.execute("ROLLBACK")
        except Exception as rollback_err:
            log(f"ROLLBACK failed: {rollback_err}")
        
        try:
            cursor.close()
        except Exception as close_err:
            log(f"cursor.close() failed: {close_err}")
            
        result_error = {
            "type": type(e).__name__,
            "message": str(e),
            "args": list(e.args)
        }
        return False, result_error


def run_transactions():
    config = helper.get_config()
    log(f"Starting transaction generator against {config['host']}:{config['port']}")
    log(f"Keyspace: {config['keyspace']}")

    # Connect to vtgate
    conn = helper.get_connection()

    # Get actual account IDs
    account_ids = get_account_ids(conn)
    if len(account_ids) == 0:
        log("No accounts found.")
        conn.close()
        return True

    log(f"Found {len(account_ids)} accounts")

    # Run random number of transactions (1-100)
    iterations = (get_random() % 100) + 1
    log(f"Running {iterations} transactions...")

    success_count = 0
    fail_count = 0

    for i in range(iterations):
        sender = account_ids[get_random() % len(account_ids)]
        recipient = account_ids[get_random() % len(account_ids)]

        # Skip if sender and recipient are the same
        if sender == recipient:
            continue

        value = get_random() % int(1e9)

        success, error = transfer(conn, sender, recipient, value)

        if success:
            success_count += 1
            log(f"  Transfer #{i}: {sender} -> {recipient}, amount={value}: OK")
        else:
            fail_count += 1
            log(f"  Transfer #{i}: {sender} -> {recipient}, amount={value}: FAILED ({error})")

    conn.close()
    log("Connection closed")
    log(f"Completed: {success_count} succeeded, {fail_count} failed")
    return True


if __name__ == '__main__':
    success = run_transactions()
    sys.exit(0 if success else 1)