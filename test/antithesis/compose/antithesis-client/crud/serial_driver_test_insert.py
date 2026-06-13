#!/usr/bin/env -S python3 -u

"""
Vitess Test - Insert Operations

This test:
1. Connects to VTGate
2. Creates test table if needed
3. Inserts multiple rows, tracking expected state in-memory
4. Queries back all rows and verifies against expected state
5. Uses Antithesis assertions for verification
"""
import sys
import random
import string
from antithesis.assertions import always, unreachable, sometimes

sys.path.append("/opt/antithesis/resources")
import helper
from helper import log

# Test configuration
NUM_INSERTS = helper.generate_requests()
TABLE_NAME = 'test_insert'


def run_test():
    config = helper.get_config()
    log(f"Starting insert test against {config['host']}:{config['port']}")
    log(f"Keyspace: {config['keyspace']}")

    # Track expected state: {id: msg}
    expected_state = {}

    # Connect to vtgate with retry
    conn = helper.get_connection_with_retry()

    # Setup table with retry
    result, conn = helper.retry_with_reconnect(
        lambda c: helper.setup_test_table(c, TABLE_NAME), conn
    )
    success, error = result
    sometimes(success, "Successful table creation", {"error":error})

    if not success:
        log(f"Table '{TABLE_NAME}' creation failed after retries")
        try:
            conn.close()
        except Exception:
            pass
        sys.exit(1)
    else:
        log(f"Table '{TABLE_NAME}' ready")


    # Perform inserts
    log(f"Inserting {NUM_INSERTS} rows...")
    for i in range(NUM_INSERTS):
        msg = helper.generate_random_string()
        result, conn = helper.retry_with_reconnect(
            lambda c, _msg=msg: helper.insert_msg(c, _msg, TABLE_NAME), conn
        )
        success, error, lastrowid = result
        sometimes(success, "Successful insert query", {"error":error})
        if success:
            row_id = lastrowid
            expected_state[row_id] = msg
            log(f" Inserted row id={row_id}, msg='{msg}'")
        else:
            log(f" Insert msg='{msg}' failed.")

    # Verify: Query back all inserted rows
    log("Verifying inserted rows...")
    verification_passed = True

    for row_id, expected_msg in expected_state.items():

        result, conn = helper.retry_with_reconnect(
            lambda c, _rid=row_id: helper.get_msg(c, _rid, TABLE_NAME), conn
        )
        success, error, query_result = result

        sometimes(success, f"Get request successful", {"error":error})

        if not success:
            log(f" Get request failed for {row_id}")

        else:
            log(f" Get request successful for {row_id}")
            actual_id, actual_msg = query_result
            row_matches = (actual_id == row_id and actual_msg == expected_msg)
            if row_matches:
                log(f"  Verified row id={row_id}: OK")
            elif not row_matches:
                verification_passed = False
                unreachable(f"Verification query failed.",{"error":error})

    # Cleanup
    try:
        conn.close()
    except Exception:
        pass
    log("Connection closed")

    if verification_passed:
        log("TEST PASSED")
    else:
        log("TEST FAILED")
    return verification_passed

if __name__ == '__main__':
    success = run_test()
    sys.exit(0 if success else 1)
