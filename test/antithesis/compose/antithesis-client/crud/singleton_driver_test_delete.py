#!/usr/bin/env -S python3 -u

"""
Vitess Test - Delete Operations

This test:
1. Connects to VTGate
2. Creates test table if needed
3. Inserts multiple rows, tracking expected state in-memory
4. Deletes each row
5. Queries back all rows and verifies they no longer exist
6. Uses Antithesis assertions for verification
"""
import sys
from antithesis.assertions import always, unreachable, sometimes

sys.path.append("/opt/antithesis/resources")
import helper
from helper import log

# Test configuration
NUM_INSERTS = helper.generate_requests()
TABLE_NAME = 'test_delete'

ERROR_LIST = {}

def run_test():
    config = helper.get_config()
    log(f"Starting delete test against {config['host']}:{config['port']}")
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

    # Phase 2: Delete each row
    deleted_ids = []
    log(f"Deleting {len(expected_state)} rows...")
    for row_id in list(expected_state.keys()):
        result, conn = helper.retry_with_reconnect(
            lambda c, _rid=row_id: helper.delete_msg(c, _rid, TABLE_NAME), conn
        )
        success, error, rows_affected = result
        sometimes(success, "Successful delete query", {"error": error})
        if success:
            deleted_ids.append(row_id)
            log(f"  Deleted row id={row_id}")
        else:
            log(f"  Delete row id={row_id} failed.")

    # Phase 3: Verify all deleted rows no longer exist
    # Note: get_msg returns (False, {"type":"NotFound",...}, None) for missing rows.
    # That's the EXPECTED outcome here, but retry_with_reconnect treats result[0]==False
    # as failure and would burn all retries. We wrap get_msg so NotFound counts as success.
    log("Verifying deleted rows...")
    verification_passed = True

    for row_id in deleted_ids:
        def verify_deleted(c, _rid=row_id):
            success, error, result = helper.get_msg(c, _rid, TABLE_NAME)
            if not success and error.get("type") == "NotFound":
                return (True, {"type": "NotFound"}, None)
            return (success, error, result)

        result, conn = helper.retry_with_reconnect(verify_deleted, conn)
        success, error, query_result = result

        sometimes(success, "Get request successful", {"error": error})

        if not success:
            log(f"  Get request failed for {row_id}")
            continue

        if query_result is None:
            log(f"  Verified row id={row_id} deleted: OK")
        else:
            verification_passed = False
            actual_id, actual_msg = query_result
            log(f"  FAILED row id={row_id} still exists: msg='{actual_msg}'")
            unreachable(
                "Row still exists after delete",
                {"row_id": row_id, "actual_msg": actual_msg}
            )

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
