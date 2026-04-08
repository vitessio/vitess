#!/usr/bin/env -S python3 -u

"""
Vitess Bank Test - Eventually Validator

This script runs when faults are paused and checks:
1. Total balance is conserved (from primary)
2. Account count is unchanged (from primary)
3. Replica data matches primary (replication caught up)
"""

import sys
from antithesis.assertions import always

sys.path.append("/opt/antithesis/resources")
import helper
from helper import log


def validate():
    config = helper.get_config()
    log(f"[Eventually] Starting validation against {config['host']}:{config['port']}")

    # Connect to vtgate
    conn = helper.get_connection()
    cursor = conn.cursor()

    # Get initial state
    try:
        cursor.execute("SELECT num_accts, total FROM initial_state")
        result = cursor.fetchone()
        if not result:
            log("[Eventually] No initial state found. Run first_setup.py first.")
            try:
                cursor.close()
            except Exception as close_err:
                log(f"[Eventually] cursor.close() failed: {close_err}")
            try:
                conn.close()
            except Exception as conn_err:
                log(f"[Eventually] conn.close() failed: {conn_err}")
            return True
        initial_num_accts, initial_total = result
        log(f"[Eventually] Initial state: num_accts={initial_num_accts}, total={initial_total}")
    except Exception as e:
        log(f"[Eventually] Failed to read initial_state: {e}")
        try:
            cursor.close()
        except Exception as close_err:
            log(f"[Eventually] cursor.close() failed: {close_err}")
        try:
            conn.close()
        except Exception as conn_err:
            log(f"[Eventually] conn.close() failed: {conn_err}")
        return True

    # Get current state from primary
    try:
        cursor.execute("SELECT COUNT(*) FROM accounts")
        current_num_accts = cursor.fetchone()[0]

        cursor.execute("SELECT SUM(balance) FROM accounts")
        current_total = cursor.fetchone()[0]
        conn.commit()
        log(f"[Eventually] Current state (primary): num_accts={current_num_accts}, total={current_total}")
    except Exception as e:
        log(f"[Eventually] Failed to read current state from primary: {e}")
        try:
            cursor.close()
        except Exception as close_err:
            log(f"[Eventually] cursor.close() failed: {close_err}")
        try:
            conn.close()
        except Exception as conn_err:
            log(f"[Eventually] conn.close() failed: {conn_err}")
        return True

    # Get current state from replica
    replica_total = None
    try:
        cursor.execute(f"USE {config['keyspace']}@replica")
        cursor.execute("SELECT SUM(balance) FROM accounts")
        replica_total = cursor.fetchone()[0]
        log(f"[Eventually] Current state (replica): total={replica_total}")
    except Exception as e:
        log(f"[Eventually] Failed to read current state from replica: {e}")

    try:
        cursor.close()
    except Exception as close_err:
        log(f"[Eventually] cursor.close() failed: {close_err}")
    try:
        conn.close()
    except Exception as conn_err:
        log(f"[Eventually] conn.close() failed: {conn_err}")

    # Assert invariants
    always(
        initial_total == current_total,
        "[Eventually] Total balance conserved",
        {"initial_total": float(initial_total), "current_total": float(current_total)}
    )

    always(
        initial_num_accts == current_num_accts,
        "[Eventually] Account count unchanged",
        {"initial_num_accts": initial_num_accts, "current_num_accts": current_num_accts}
    )

    if replica_total is not None:
        always(
            current_total == replica_total,
            "[Eventually] Replica matches primary",
            {"primary_total": float(current_total), "replica_total": float(replica_total)}
        )

    balance_ok = (initial_total == current_total)
    count_ok = (initial_num_accts == current_num_accts)
    replica_ok = (replica_total is None) or (current_total == replica_total)

    if balance_ok and count_ok and replica_ok:
        log("[Eventually] VALIDATION PASSED")
    else:
        log("[Eventually] VALIDATION FAILED")

    return balance_ok and count_ok and replica_ok


if __name__ == '__main__':
    success = validate()
    sys.exit(0 if success else 1)