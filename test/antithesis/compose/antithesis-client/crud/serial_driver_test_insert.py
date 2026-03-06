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
    
    # Connect to vtgate
    conn = helper.get_connection()

    # Setup table
    success, error = helper.setup_test_table(conn, TABLE_NAME)
    sometimes(success, "Successful table creation", {"error":error})

    if not success:
        log(f"Table '{TABLE_NAME}' creation failed")    
        conn.close()
        sys.exit(0)
        return False
    else:
        log(f"Table '{TABLE_NAME}' ready")

    
    # Perform inserts
    log(f"Inserting {NUM_INSERTS} rows...")
    for i in range(NUM_INSERTS):
        msg = helper.generate_random_string()
        success, error, lastrowid = helper.insert_msg(conn, msg, TABLE_NAME)
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

        success, error, result = helper.get_msg(conn, row_id, TABLE_NAME)

        sometimes(success, f"Get request successful", {"error":error})

        if not success:
            log(f" Get request failed for {row_id}")

        else:
            actual_id, actual_msg = result
            row_matches = (actual_id == row_id and actual_msg == expected_msg)
            if row_matches:
                log(f"  Verified row id={row_id}: OK")
            elif not row_matches:
                verification_passed = False
                unreachable(f"Verification query failed.",{"error":error})

    # # Final count verification
    # cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE id IN ({','.join(map(str, expected_state.keys()))})")
    # actual_count = cursor.fetchone()[0]
    # expected_count = len(expected_state)
    
    # count_matches = (actual_count == expected_count)
    # always(
    #     count_matches,
    #     "Row count matches expected",
    #     {"expected": expected_count, "actual": actual_count}
    # )
    
    # if count_matches:
    #     log(f"Row count verification: OK ({actual_count} rows)")
    # else:
    #     log(f"Row count MISMATCH: expected={expected_count}, actual={actual_count}")
    #     verification_passed = False
    
    # Cleanup
    conn.close()
    log("Connection closed")
    
    if verification_passed:
        log("TEST PASSED")
    else:
        log("TEST FAILED")
    
    return verification_passed

if __name__ == '__main__':
    success = run_test()
    sys.exit(0 if success else 1)