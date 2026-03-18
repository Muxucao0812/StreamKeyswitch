#!/usr/bin/env python3
"""Plot Poseidon KeySwitch execution: BRAM occupancy + operation Gantt chart."""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import subprocess
import sys
import re

def run_sim():
    cmd = [
        "./build-cmake/keyaware_sim",
        "--scheduler", "fifo",
        "--backend", "cycle_stub",
        "--workload", "synthetic",
        "--ks-method", "poseidon",
        "--disable-multi-card",
        "--seed", "42",
        "--num-users", "1",
        "--requests-per-user", "1",
        "--inter-arrival", "10",
        "--num-cards", "4",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout

def parse_bram_timeline(output):
    cycles, bram = [], []
    in_section = False
    for line in output.split('\n'):
        if '# cycle,bram_bytes' in line:
            in_section = True
            continue
        if in_section:
            if line.startswith('---') or line.startswith('===') or not line.strip():
                break
            parts = line.strip().split(',')
            if len(parts) == 2:
                cycles.append(int(parts[0]))
                bram.append(int(parts[1]))
    return np.array(cycles), np.array(bram)

def parse_group_timeline(output):
    groups = []
    in_section = False
    for line in output.split('\n'):
        if 'ID  Name' in line:
            in_section = True
            continue
        if in_section:
            if line.startswith('\n---') or line.startswith('===') or (not line.strip()):
                break
            if line.strip().startswith('---'):
                break
            parts = line.split()
            if len(parts) >= 8 and parts[0].isdigit():
                groups.append({
                    'id': int(parts[0]),
                    'name': parts[1],
                    'start': int(parts[2]),
                    'end': int(parts[3]),
                    'duration': int(parts[4]),
                    'bram_before': int(parts[5]),
                    'bram_after': int(parts[6]),
                    'bytes': int(parts[7]),
                })
    return groups

def parse_cycle_breakdown(output):
    breakdown = {}
    in_section = False
    for line in output.split('\n'):
        if 'Cycle Breakdown by Instruction Kind' in line:
            in_section = True
            continue
        if in_section:
            if line.startswith('\n---') or line.startswith('===') or not line.strip():
                break
            if line.strip().startswith('---'):
                break
            m = re.match(r'\s+(\w+):\s+cycles=(\d+)\s+\(([\d.]+)%\)', line)
            if m:
                breakdown[m.group(1)] = {
                    'cycles': int(m.group(2)),
                    'pct': float(m.group(3)),
                }
    return breakdown

def main():
    output = run_sim()

    cycles, bram = parse_bram_timeline(output)
    groups = parse_group_timeline(output)
    breakdown = parse_cycle_breakdown(output)

    if len(cycles) == 0:
        print("No BRAM timeline data found!")
        sys.exit(1)

    bram_mb = bram / (1024 * 1024)
    total_cycles = max(g['end'] for g in groups) if groups else cycles[-1]

    color_map = {
        'load_input': '#4CAF50',
        'modup_intt': '#2196F3',
        'modup_bconv': '#FF9800',
        'modup_ntt': '#9C27B0',
        'modup_spill': '#F44336',
        'innerprod_reload_modup': '#8BC34A',
        'innerprod_load_key': '#00BCD4',
        'innerprod_mul': '#E91E63',
        'innerprod_add': '#FF5722',
        'innerprod_spill_accum': '#795548',
        'moddown_intt': '#3F51B5',
        'moddown_bconv': '#FFC107',
        'moddown_ntt': '#673AB7',
        'moddown_reload_2l': '#009688',
        'moddown_subtract': '#CDDC39',
        'moddown_store_output': '#607D8B',
    }

    fig, axes = plt.subplots(3, 1, figsize=(18, 14), height_ratios=[3, 2, 1.5])
    fig.suptitle('Poseidon KeySwitch Execution Profile', fontsize=16, fontweight='bold')

    # --- Plot 1: BRAM Occupancy over time ---
    ax1 = axes[0]
    ax1.fill_between(cycles, bram_mb, alpha=0.3, color='steelblue')
    ax1.plot(cycles, bram_mb, color='steelblue', linewidth=1.5)
    ax1.set_ylabel('BRAM Occupancy (MB)', fontsize=12)
    ax1.set_xlabel('Cycle', fontsize=12)
    ax1.set_title('BRAM Occupancy Over Time', fontsize=13)
    ax1.set_xlim(0, total_cycles)
    ax1.grid(True, alpha=0.3)
    ax1.axhline(y=31876710 / (1024*1024), color='red', linestyle='--', alpha=0.5, label='Budget (30.4 MB)')
    ax1.legend()

    # --- Plot 2: Gantt chart of operations ---
    ax2 = axes[1]
    unique_names = list(dict.fromkeys(g['name'] for g in groups))
    name_to_y = {name: i for i, name in enumerate(unique_names)}

    for g in groups:
        y = name_to_y[g['name']]
        color = color_map.get(g['name'], '#999999')
        ax2.barh(y, g['duration'], left=g['start'], height=0.7,
                color=color, edgecolor='black', linewidth=0.5, alpha=0.8)

    ax2.set_yticks(range(len(unique_names)))
    ax2.set_yticklabels(unique_names, fontsize=9)
    ax2.set_xlabel('Cycle', fontsize=12)
    ax2.set_title('Operation Gantt Chart', fontsize=13)
    ax2.set_xlim(0, total_cycles)
    ax2.grid(True, axis='x', alpha=0.3)
    ax2.invert_yaxis()

    # --- Plot 3: Cycle breakdown pie chart ---
    ax3 = axes[2]
    if breakdown:
        labels = list(breakdown.keys())
        sizes = [breakdown[k]['cycles'] for k in labels]
        pie_colors = ['#2196F3', '#F44336', '#9C27B0', '#4CAF50', '#E91E63', '#FF9800', '#00BCD4', '#795548']
        wedges, texts, autotexts = ax3.pie(
            sizes, labels=labels, autopct='%1.1f%%',
            colors=pie_colors[:len(labels)],
            startangle=90, textprops={'fontsize': 9})
        ax3.set_title('Cycle Breakdown by Operation Type', fontsize=13)

    plt.tight_layout()
    out_path = 'results/poseidon_execution_profile.png'
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    print(f"Saved to {out_path}")
    plt.show()

if __name__ == '__main__':
    main()
