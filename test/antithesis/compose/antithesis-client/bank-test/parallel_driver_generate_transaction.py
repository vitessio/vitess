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


def get_num_accounts(conn):
    """Get number of accounts from initial_state table."""
    cursor = conn.cursor()
    result = None
    try:
        cursor.execute("SELECT num_accts FROM initial_state")
        result = cursor.fetchone()
    except Exception as e:
        log(f"failed to get accounts: {e}")
    finally:
        try:
            cursor.close()
        except Exception as close_err:
            log(f"cursor.close() failed: {close_err}")
    return result[0] if result else 0


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

    # Get number of accounts
    num_accts = get_num_accounts(conn)
    if num_accts == 0:
        log("No accounts found.")
        conn.close()
        return True

    log(f"Found {num_accts} accounts")

    # Run random number of transactions (1-100)
    iterations = (get_random() % 100) + 1
    log(f"Running {iterations} transactions...")

    success_count = 0
    fail_count = 0

    for i in range(iterations):
        sender = (get_random() % num_accts) + 1
        recipient = (get_random() % num_accts) + 1

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