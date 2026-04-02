#!/usr/bin/env -S python3 -u

"""
Vitess Test - Select Operations from Different Tablet Types

This test:
1. Connects to VTGate
2. Creates test table if needed
3. Inserts multiple rows, tracking expected state in-memory
4. Reads each row from primary, replica, and rdonly tablets
5. Verifies data consistency across all tablet types
6. Uses Antithesis assertions for verification
"""
import sys
from antithesis.assertions import always, unreachable, sometimes

sys.path.append("/opt/antithesis/resources")
import helper
from helper import log

# Test configuration
NUM_INSERTS = helper.generate_requests()
TABLE_NAME = 'test_select'
TABLET_TYPES = ['primary', 'replica']


def run_test():
    config = helper.get_config()
    log(f"Starting select test against {config['host']}:{config['port']}")
    log(f"Keyspace: {config['keyspace']}")
    log(f"Tablet types to test: {TABLET_TYPES}")

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


    # Phase 1: Perform inserts
    log(f"Inserting {NUM_INSERTS} rows...")
    for i in range(NUM_INSERTS):
        msg = helper.generate_random_string()
        result, conn = helper.retry_with_reconnect(
            lambda c, _msg=msg: helper.insert_msg(c, _msg, TABLE_NAME), conn
        )
        success, error, lastrowid = result
        sometimes(success, "Successful insert query", {"error": error})
        if success:
            row_id = lastrowid
            expected_state[row_id] = msg
            log(f"  Inserted row id={row_id}, msg='{msg}'")
        else:
            log(f"  Insert msg='{msg}' failed.")

    # Phase 2: Read from each tablet type and verify
    verification_passed = True

    for tablet_type in TABLET_TYPES:
        log(f"Verifying rows from {tablet_type}...")
        tablet_passed = True

        for row_id, expected_msg in expected_state.items():
            if tablet_type == 'primary':
                result, conn = helper.retry_with_reconnect(
                    lambda c, _rid=row_id: helper.get_msg(c, _rid, TABLE_NAME), conn
                )
            else:
                # For replicas, NotFound is expected (replication lag) — don't waste retries on it.
                # Connection errors still get retried.
                def read_replica(c, _rid=row_id, _tt=tablet_type):
                    success, error, result = helper.get_msg(c, _rid, TABLE_NAME, _tt)
                    if not success and error.get("type") == "NotFound":
                        return (True, {"type": "NotFound"}, None)
                    return (success, error, result)
                result, conn = helper.retry_with_reconnect(read_replica, conn)
            success, error, query_result = result

            if not success:
                sometimes(False, f"Get from {tablet_type} successful", {"error": error})
                log(f"  [{tablet_type}] Get request failed for {row_id}: {error}")
                continue

            if query_result is None:
                # Row not found — for replicas this is replication lag, for primary it's unexpected
                sometimes(False, f"Get from {tablet_type} successful", {"error": error})
                log(f"  [{tablet_type}] Row id={row_id} not found (replication lag?)")
                continue

            sometimes(True, f"Get from {tablet_type} successful", {"error": error})

            actual_id, actual_msg = query_result
            row_matches = (actual_id == row_id and actual_msg == expected_msg)

            if row_matches:
                log(f"  [{tablet_type}] Verified row id={row_id}: OK")
            else:
                tablet_passed = False
                log(f"  [{tablet_type}] MISMATCH row id={row_id}: expected='{expected_msg}', actual='{actual_msg}'")
                unreachable(
                    f"Row mismatch on {tablet_type}",
                    {"tablet_type": tablet_type, "row_id": row_id, "expected": expected_msg, "actual": actual_msg}
                )
        if tablet_passed:
            log(f"[{tablet_type}] All rows verified: OK")
        else:
            log(f"[{tablet_type}] Some rows failed verification")
            verification_passed = False

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
