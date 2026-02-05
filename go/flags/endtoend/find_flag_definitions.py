#!/usr/bin/env python3
"""
Find all flag definitions with underscores in the Vitess codebase and group them.
"""

import os
import re
import json
from collections import defaultdict
import subprocess

def find_flag_definitions():
    """Find all flag definitions with underscores in Go files."""
    
    # Load the flags we identified from help text
    vtroot = os.environ.get('VTROOT')
    if not vtroot:
        print("Error: VTROOT environment variable is not set. Please set it to the Vitess root directory.")
        exit(1)
    with open(f'{vtroot}/go/flags/endtoend/flags_analysis.json', 'r') as f:
        analysis = json.load(f)
    
    underscore_flags = analysis['unique_underscore_flags']
    flag_to_binaries = analysis['flag_to_binaries']
    
    # Group flags by category
    flag_groups = {
        'logging': [],
        'backup_restore': [],
        'stats_monitoring': [],
        'database': [],
        'ddl': [],
        'buffer': [],
        'timeout': [],
        'topo': [],
        'grpc': [],
        'test': [],
        'other': []
    }
    
    flag_definitions = {}
    
    # Search for each flag definition
    for flag in underscore_flags:
        # Skip glog flags (they come from standard library)
        if flag.startswith('log_'):
            flag_groups['logging'].append(flag)
            continue
            
        # Try to find the definition
        cmd = f'grep -r "\\"{flag}\\"" {vtroot}/go --include="*.go" -n | head -5'
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=5)
            if result.stdout:
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    if 'Var(' in line or 'AddGoFlag' in line:
                        flag_definitions[flag] = line.strip()
                        break
        except:
            pass
        
        # Categorize the flag
        if 'backup' in flag or 'restore' in flag:
            flag_groups['backup_restore'].append(flag)
        elif 'stats' in flag or 'statsd' in flag or 'metric' in flag:
            flag_groups['stats_monitoring'].append(flag)
        elif 'mysql' in flag or 'db_' in flag or 'database' in flag:
            flag_groups['database'].append(flag)
        elif 'ddl' in flag:
            flag_groups['ddl'].append(flag)
        elif 'buffer' in flag:
            flag_groups['buffer'].append(flag)
        elif 'timeout' in flag or 'deadline' in flag:
            flag_groups['timeout'].append(flag)
        elif 'topo' in flag or 'cell' in flag:
            flag_groups['topo'].append(flag)
        elif 'grpc' in flag:
            flag_groups['grpc'].append(flag)
        elif 'test' in flag:
            flag_groups['test'].append(flag)
        else:
            flag_groups['other'].append(flag)
    
    return flag_groups, flag_definitions, flag_to_binaries

def print_migration_plan():
    """Print a structured migration plan."""
    vtroot = os.environ.get('VTROOT')
    if not vtroot:
        print("Error: VTROOT environment variable is not set. Please set it to the Vitess root directory.")
        exit(1)
    flag_groups, flag_definitions, flag_to_binaries = find_flag_definitions()
    
    print("=" * 80)
    print("FLAGS MIGRATION PLAN - GROUPED BY FUNCTION")
    print("=" * 80)
    print()
    
    # Priority order for groups
    priority_order = [
        ('logging', 'Logging flags (glog - may not need migration)'),
        ('timeout', 'Timeout and deadline flags'),
        ('backup_restore', 'Backup and restore flags'),
        ('database', 'Database connection flags'),
        ('ddl', 'DDL operation flags'),
        ('buffer', 'Buffering flags'),
        ('stats_monitoring', 'Stats and monitoring flags'),
        ('topo', 'Topology and cell flags'),
        ('grpc', 'gRPC configuration flags'),
        ('test', 'Test-specific flags'),
        ('other', 'Other/miscellaneous flags')
    ]
    
    total_flags = sum(len(flags) for flags in flag_groups.values())
    
    for group_name, description in priority_order:
        flags = flag_groups[group_name]
        if not flags:
            continue
            
        print(f"GROUP: {description}")
        print(f"Count: {len(flags)} flags")
        print("-" * 40)
        
        for flag in sorted(flags):
            binaries = flag_to_binaries.get(flag, [])
            binary_list = ', '.join(binaries[:3])
            if len(binaries) > 3:
                binary_list += f' + {len(binaries)-3} more'
            print(f"  {flag:<40} [{binary_list}]")
            
            # Show definition if found
            if flag in flag_definitions:
                definition = flag_definitions[flag]
                if len(definition) > 100:
                    definition = definition[:97] + "..."
                print(f"    -> {definition}")
        
        print()
    
    # Save the migration plan
    migration_plan = {
        'groups': {name: flags for name, flags in flag_groups.items() if flags},
        'definitions': flag_definitions,
        'priority_order': [name for name, _ in priority_order],
        'total_flags': total_flags
    }
    
    with open(f'{vtroot}/go/flags/endtoend/migration_plan.json', 'w') as f:
        json.dump(migration_plan, f, indent=2)
    
    print("=" * 80)
    print(f"TOTAL FLAGS TO MIGRATE: {total_flags}")
    print("Migration plan saved to: migration_plan.json")
    print()
    print("RECOMMENDED APPROACH:")
    print("1. Start with timeout flags (small group, high impact)")
    print("2. Then backup_restore flags (medium group, isolated)")
    print("3. Continue with other groups in order")
    print("4. Skip logging flags if they're from glog")

if __name__ == "__main__":
    print_migration_plan()