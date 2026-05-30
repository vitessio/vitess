#!/usr/bin/env -S python3 -u

"""
Vitess Bank Test - Anytime Validator

This script runs during chaos/fault injection and checks:
1. Total balance is conserved (from primary)
2. Account count is unchanged (from primary)
"""

import sys
from antithesis.assertions import always

sys.path.append("/opt/antithesis/resources")
import helper
from helper import log


def validate():
    config = helper.get_config()
    log(f"[Anytime] Starting validation against {config['host']}:{config['port']}")

    # Connect to vtgate
    conn = helper.get_connection()
    cursor = conn.cursor()

    # Get initial state
    try:
        cursor.execute("SELECT num_accts, total FROM initial_state")
        result = cursor.fetchone()
        if not result:
            log("[Anytime] No initial state found. Run first_setup.py first.")
            try:
                cursor.close()
            except Exception as close_err:
                log(f"[Anytime] cursor.close() failed: {close_err}")
            try:
                conn.close()
            except Exception as conn_err:
                log(f"[Anytime] conn.close() failed: {conn_err}")
            return False
        initial_num_accts, initial_total = result
        log(f"[Anytime] Initial state: num_accts={initial_num_accts}, total={initial_total}")
    except Exception as e:
        log(f"[Anytime] Failed to read initial_state: {e}")
        try:
            cursor.close()
        except Exception as close_err:
            log(f"[Anytime] cursor.close() failed: {close_err}")
        try:
            conn.close()
        except Exception as conn_err:
            log(f"[Anytime] conn.close() failed: {conn_err}")
        return True

    # Get current state from primary
    try:
        cursor.execute("SELECT COUNT(*) FROM accounts")
        current_num_accts = cursor.fetchone()[0]

        cursor.execute("SELECT SUM(balance) FROM accounts")
        current_total = cursor.fetchone()[0]
        log(f"[Anytime] Current state (primary): num_accts={current_num_accts}, total={current_total}")
    except Exception as e:
        log(f"[Anytime] Failed to read current state: {e}")
        try:
            cursor.close()
        except Exception as close_err:
            log(f"[Anytime] cursor.close() failed: {close_err}")
        try:
            conn.close()
        except Exception as conn_err:
            log(f"[Anytime] conn.close() failed: {conn_err}")
        return True

    try:
        cursor.close()
    except Exception as close_err:
        log(f"[Anytime] cursor.close() failed: {close_err}")
    try:
        conn.close()
    except Exception as conn_err:
        log(f"[Anytime] conn.close() failed: {conn_err}")

    # Assert invariants
    always(
        initial_total == current_total,
        "[Anytime] Total balance conserved",
        {"initial_total": float(initial_total), "current_total": float(current_total)}
    )

    always(
        initial_num_accts == current_num_accts,
        "[Anytime] Account count unchanged",
        {"initial_num_accts": initial_num_accts, "current_num_accts": current_num_accts}
    )

    balance_ok = (initial_total == current_total)
    count_ok = (initial_num_accts == current_num_accts)

    if balance_ok and count_ok:
        log("[Anytime] VALIDATION PASSED")
    else:
        log("[Anytime] VALIDATION FAILED")

    return balance_ok and count_ok


if __name__ == '__main__':
    success = validate()
    sys.exit(0 if success else 1)