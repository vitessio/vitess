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
TABLET_TYPES = ['primary', 'replica', 'rdonly']


def run_test():
    config = helper.get_config()
    log(f"Starting select test against {config['host']}:{config['port']}")
    log(f"Keyspace: {config['keyspace']}")
    log(f"Tablet types to test: {TABLET_TYPES}")
    
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

    
    # Phase 1: Perform inserts
    log(f"Inserting {NUM_INSERTS} rows...")
    for i in range(NUM_INSERTS):
        msg = helper.generate_random_string()
        success, error, lastrowid = helper.insert_msg(conn, msg, TABLE_NAME)
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
            success, error, result = helper.get_msg(
                conn, row_id, TABLE_NAME, tablet_type
            )
            
            # Replica/rdonly may have replication lag, so use sometimes
            sometimes(success, f"Get from {tablet_type} successful", {"error": error})

            if not success:
                log(f"  [{tablet_type}] Get request failed for {row_id}: {error}")
                continue

            if result is None:
                log(f"  [{tablet_type}] Row id={row_id} not found (replication lag?)")
                continue

            actual_id, actual_msg = result
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
    conn.close()
    log("Connection closed")
    
    if verification_passed:
        log("TEST PASSED")
    else:
        log("TEST FAILED")
    
    return verification_passed


if __name__ == '__main__':
    success = run_test()
    sys.exit(0)