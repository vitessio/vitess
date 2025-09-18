#!/usr/bin/env python3
"""
Comprehensive flag analysis tool for Vitess flags migration project.
Analyzes help text files to identify flags with underscores that need migration.
"""

import os
import re
from collections import defaultdict
import json

def parse_help_file(filepath):
    """Parse a help text file and extract flag information."""
    flags = []
    binary_name = os.path.basename(filepath).replace('.txt', '')
    
    with open(filepath, 'r') as f:
        lines = f.readlines()
    
    reading_flags = False
    for line in lines:
        # Check if we're entering the flags section
        if line.strip() in ["Flags:", "Usage of vtctlclient:"]:
            reading_flags = True
            continue
        
        if reading_flags:
            # Extract flag if line starts with --
            match = re.match(r'\s*(--[a-zA-Z0-9_-]+)', line)
            if match:
                flag_name = match.group(1)[2:]  # Remove --
                has_underscore = '_' in flag_name
                has_dash = '-' in flag_name
                
                flags.append({
                    'name': flag_name,
                    'has_underscore': has_underscore,
                    'has_dash': has_dash,
                    'binary': binary_name
                })
    
    return flags

def analyze_all_binaries():
    """Analyze all binary help files and generate comprehensive report."""
    vtroot = os.environ.get('VTROOT')
    if not vtroot:
        print("Error: VTROOT environment variable is not set. Please set it to the Vitess root directory.")
        exit(1)
    flags_dir = f'{vtroot}/go/flags/endtoend'
    
    # Target binaries we care about
    target_binaries = [
        'vtctld', 'vtctldclient', 'vttablet', 'vtgate', 'vtorc',
        'mysqlctl', 'mysqlctld', 'vtcombo', 'vttestserver', 
        'vtbackup', 'vtclient', 'vtbench'
    ]
    
    all_flags = []
    binary_stats = {}
    flag_to_binaries = defaultdict(set)
    
    # Process each help file
    for filename in os.listdir(flags_dir):
        if filename.endswith('.txt'):
            binary_name = filename.replace('.txt', '')
            if binary_name not in target_binaries:
                continue
                
            filepath = os.path.join(flags_dir, filename)
            flags = parse_help_file(filepath)
            all_flags.extend(flags)
            
            # Calculate stats for this binary
            underscore_count = sum(1 for f in flags if f['has_underscore'])
            dash_count = sum(1 for f in flags if f['has_dash'] and not f['has_underscore'])
            
            binary_stats[binary_name] = {
                'total_flags': len(flags),
                'underscore_flags': underscore_count,
                'dash_flags': dash_count,
                'completion_percentage': round((dash_count / len(flags) * 100) if flags else 100, 1)
            }
            
            # Map flags to binaries
            for flag in flags:
                flag_to_binaries[flag['name']].add(binary_name)
    
    return all_flags, binary_stats, flag_to_binaries

def generate_report():
    """Generate comprehensive migration report."""
    vtroot = os.environ.get('VTROOT')
    if not vtroot:
        print("Error: VTROOT environment variable is not set. Please set it to the Vitess root directory.")
        exit(1)
    all_flags, binary_stats, flag_to_binaries = analyze_all_binaries()
    
    print("=" * 80)
    print("VITESS FLAGS MIGRATION STATUS REPORT")
    print("=" * 80)
    print()
    
    # Overall statistics
    total_underscore = sum(s['underscore_flags'] for s in binary_stats.values())
    total_dash = sum(s['dash_flags'] for s in binary_stats.values())
    total_flags = sum(s['total_flags'] for s in binary_stats.values())
    
    print("OVERALL PROGRESS:")
    print(f"  Total flags: {total_flags}")
    print(f"  Flags with underscores (need migration): {total_underscore}")
    print(f"  Flags with dashes (completed): {total_dash}")
    print(f"  Overall completion: {round((total_dash / total_flags * 100) if total_flags else 100, 1)}%")
    print()
    
    # Per-binary statistics
    print("PER-BINARY STATUS:")
    print("-" * 80)
    print(f"{'Binary':<15} {'Total':<10} {'Underscore':<12} {'Dash':<10} {'Completion':<12}")
    print("-" * 80)
    
    # Sort by number of underscore flags (most work needed first)
    sorted_binaries = sorted(binary_stats.items(), 
                            key=lambda x: x[1]['underscore_flags'], 
                            reverse=True)
    
    for binary, stats in sorted_binaries:
        status = "✓ DONE" if stats['underscore_flags'] == 0 else ""
        print(f"{binary:<15} {stats['total_flags']:<10} {stats['underscore_flags']:<12} "
              f"{stats['dash_flags']:<10} {stats['completion_percentage']:<10.1f}% {status}")
    
    print()
    
    # Find unique underscore flags across all binaries
    unique_underscore_flags = set()
    for flag in all_flags:
        if flag['has_underscore']:
            unique_underscore_flags.add(flag['name'])
    
    print(f"UNIQUE FLAGS WITH UNDERSCORES: {len(unique_underscore_flags)}")
    print("-" * 80)
    
    # Group flags by how many binaries use them
    flag_usage = defaultdict(list)
    for flag_name in unique_underscore_flags:
        binaries = flag_to_binaries[flag_name]
        flag_usage[len(binaries)].append((flag_name, binaries))
    
    # Show flags used by multiple binaries first (higher impact)
    for count in sorted(flag_usage.keys(), reverse=True):
        if count > 1:
            print(f"\nFlags used by {count} binaries:")
            for flag_name, binaries in sorted(flag_usage[count]):
                print(f"  {flag_name:<40} -> {', '.join(sorted(binaries))}")
    
    # Save detailed data to JSON for further processing
    detailed_data = {
        'binary_stats': binary_stats,
        'unique_underscore_flags': list(unique_underscore_flags),
        'flag_to_binaries': {k: list(v) for k, v in flag_to_binaries.items() if '_' in k}
    }
    
    with open(f'{vtroot}/go/flags/endtoend/flags_analysis.json', 'w') as f:
        json.dump(detailed_data, f, indent=2)
    
    print()
    print("Detailed analysis saved to: flags_analysis.json")
    print()
    
    # Suggest migration priority
    print("SUGGESTED MIGRATION PRIORITY:")
    print("-" * 80)
    print("1. High-impact flags (used by multiple binaries)")
    print("2. Binaries with most underscore flags remaining:")
    for i, (binary, stats) in enumerate(sorted_binaries[:5], 1):
        if stats['underscore_flags'] > 0:
            print(f"   {i}. {binary} ({stats['underscore_flags']} flags)")

if __name__ == "__main__":
    generate_report()