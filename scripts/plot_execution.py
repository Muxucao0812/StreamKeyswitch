#!/usr/bin/env python3
"""Plot KeySwitch execution profile: BRAM occupancy + operation Gantt chart."""

import argparse
import matplotlib.pyplot as plt
import numpy as np
import subprocess
import sys
import re
from pathlib import Path


PRIMITIVE_ORDER = [
    'LoadHBM',
    'StoreHBM',
    'Decompose',
    'NTT',
    'INTT',
    'BConv',
    'EweMul',
    'EweAdd',
    'EweSub',
    'InterCardComm',
    'Other',
]

PRIMITIVE_COLORS = {
    'LoadHBM': '#43A047',
    'StoreHBM': '#E53935',
    'Decompose': '#6D4C41',
    'NTT': '#1E88E5',
    'INTT': '#3949AB',
    'BConv': '#FB8C00',
    'EweMul': '#8E24AA',
    'EweAdd': '#00ACC1',
    'EweSub': '#7CB342',
    'InterCardComm': '#5E35B1',
    'Other': '#757575',
}


def prettify_label(name):
    return name.replace('_', ' ')


def aggregate_breakdown_items(breakdown, small_pct_threshold=3.0):
    if not breakdown:
        return [], []

    items = sorted(
        ((kind, info['cycles']) for kind, info in breakdown.items()),
        key=lambda item: item[1],
        reverse=True,
    )
    total_cycles = sum(cycles for _, cycles in items)
    if total_cycles == 0:
        return [], []

    major = []
    other_cycles = 0
    for label, cycles in items:
        pct = 100.0 * cycles / total_cycles
        if label == 'Other' or pct < small_pct_threshold:
            other_cycles += cycles
        else:
            major.append((label, cycles))

    if other_cycles > 0:
        major.append(('Other', other_cycles))

    labels = [label for label, _ in major]
    sizes = [cycles for _, cycles in major]
    return labels, sizes


def pie_autopct(min_pct=5.0):
    def _formatter(pct):
        return f'{pct:.1f}%' if pct >= min_pct else ''
    return _formatter

def run_sim(args):
    sim_args = [
        "--scheduler", "fifo",
        "--backend", "cycle_stub",
        "--workload", "synthetic",
        "--ks-method", args.ks_method,
        "--seed", str(args.seed),
        "--num-users", str(args.num_users),
        "--requests-per-user", str(args.requests_per_user),
        "--inter-arrival", str(args.inter_arrival),
        "--num-cards", str(args.num_cards),
    ]
    sim_args.append("--enable-multi-card" if args.enable_multi_card else "--disable-multi-card")
    sim_binaries = [
        "./build-cmake/keyaware_sim",
        "./build/keyaware_sim",
        "./main",
    ]
    errors = []
    for sim_bin in sim_binaries:
        cmd = [sim_bin] + sim_args
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
        except (FileNotFoundError, OSError) as exc:
            errors.append(f"{sim_bin}: {exc}")
            continue
        if result.returncode == 0 and result.stdout:
            return result.stdout
        error_detail = result.stderr.strip() or f"exit code {result.returncode}"
        errors.append(f"{sim_bin}: {error_detail}")

    raise RuntimeError(
        "Failed to run simulator with available binaries:\n"
        + "\n".join(errors)
    )

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


def parse_operation_kind_map(output):
    mapping = {}
    in_section = False
    for line in output.split('\n'):
        if 'EstimateCycle (1-Limb Input) by Operation' in line:
            in_section = True
            continue
        if in_section:
            if line.startswith('\n---') or line.startswith('===') or not line.strip():
                break
            if line.strip().startswith('---'):
                break
            m = re.match(r'\s+([^:]+):\s+kind=([A-Za-z0-9_]+)\s+', line)
            if m:
                mapping[m.group(1).strip()] = m.group(2).strip()
    return mapping


def parse_emitted_group_trace(output):
    old_format = re.compile(
        r'\[group\s+(\d+)\]\s+([^\s]+)\s+kind=([A-Za-z0-9_]+).*?'
        r'total_cycles=(\d+)\s+bytes=(\d+)\s+bram_live=(\d+)')
    new_format = re.compile(
        r'Emitted group\s+(\d+):\s+([^,]+),\s+kind=([A-Za-z0-9_]+),\s+'
        r'bytes=(\d+),.*?total_est_cycles=(\d+),\s+bram_live=(\d+)')

    kind_map = {
        '0': 'LoadHBM',
        '1': 'StoreHBM',
        '2': 'NTT',
        '3': 'INTT',
        '4': 'EweMul',
        '5': 'EweAdd',
        '6': 'EweSub',
        '7': 'BConv',
        '8': 'InterCardSend',
        '9': 'InterCardRecv',
    }

    rows = []
    for line in output.split('\n'):
        m_old = old_format.search(line)
        if m_old is not None:
            rows.append({
                'id': int(m_old.group(1)),
                'name': m_old.group(2),
                'kind': kind_map.get(m_old.group(3), m_old.group(3)),
                'duration': int(m_old.group(4)),
                'bytes': int(m_old.group(5)),
                'bram_after': int(m_old.group(6)),
            })
            continue

        m_new = new_format.search(line)
        if m_new is not None:
            rows.append({
                'id': int(m_new.group(1)),
                'name': m_new.group(2),
                'kind': kind_map.get(m_new.group(3), m_new.group(3)),
                'duration': int(m_new.group(5)),
                'bytes': int(m_new.group(4)),
                'bram_after': int(m_new.group(6)),
            })

    if not rows:
        return None

    raw_groups = sorted(rows, key=lambda g: g['id'])

    groups = []
    cycles = [0]
    bram = [0]
    breakdown = {}
    current_cycle = 0
    current_live = 0
    for g in raw_groups:
        start = current_cycle
        end = start + g['duration']
        groups.append({
            'id': g['id'],
            'name': g['name'],
            'kind': g['kind'],
            'start': start,
            'end': end,
            'duration': g['duration'],
            'bram_before': current_live,
            'bram_after': g['bram_after'],
            'bytes': g['bytes'],
        })
        current_cycle = end
        current_live = g['bram_after']
        cycles.append(end)
        bram.append(current_live)
        breakdown[g['kind']] = {
            'cycles': breakdown.get(g['kind'], {'cycles': 0})['cycles'] + g['duration'],
            'pct': 0.0,
        }

    total_cycles = max(1, sum(info['cycles'] for info in breakdown.values()))
    for info in breakdown.values():
        info['pct'] = 100.0 * info['cycles'] / total_cycles

    return np.array(cycles), np.array(bram), groups, breakdown


def normalize_primitive_kind(kind):
    if kind in ('InterCardSend', 'InterCardRecv', 'InterCardReduce'):
        return 'InterCardComm'
    if kind in PRIMITIVE_COLORS:
        return kind
    return 'Other'


def infer_primitive_from_name(name):
    lower_name = name.lower()
    if 'intercard' in lower_name or (
        ('send' in lower_name or 'recv' in lower_name) and 'card' in lower_name
    ):
        return 'InterCardComm'
    if 'store' in lower_name or 'spill' in lower_name:
        return 'StoreHBM'
    if 'load' in lower_name or 'reload' in lower_name:
        return 'LoadHBM'
    if 'bconv' in lower_name:
        return 'BConv'
    if 'intt' in lower_name:
        return 'INTT'
    if 'ntt' in lower_name:
        return 'NTT'
    if 'decompose' in lower_name:
        return 'Decompose'
    if 'sub' in lower_name:
        return 'EweSub'
    if 'add' in lower_name or 'reduce' in lower_name or 'accumulate' in lower_name:
        return 'EweAdd'
    if 'mul' in lower_name or 'innerprod' in lower_name:
        return 'EweMul'
    return 'Other'


def classify_groups_by_primitive(groups, operation_kind_map):
    for group in groups:
        kind = group.get('kind')
        if not kind:
            kind = operation_kind_map.get(group.get('name', ''))
        if not kind:
            kind = infer_primitive_from_name(group.get('name', ''))
        group['primitive'] = normalize_primitive_kind(kind)
    return groups


def build_primitive_breakdown(groups):
    breakdown = {}
    for group in groups:
        primitive = group.get('primitive', 'Other')
        duration = int(group.get('duration', 0))
        if duration <= 0:
            continue
        entry = breakdown.setdefault(primitive, {'cycles': 0, 'pct': 0.0})
        entry['cycles'] += duration

    total_cycles = sum(entry['cycles'] for entry in breakdown.values())
    if total_cycles > 0:
        for entry in breakdown.values():
            entry['pct'] = 100.0 * entry['cycles'] / total_cycles
    return breakdown


def primitive_sort_key(kind):
    try:
        return (PRIMITIVE_ORDER.index(kind), kind)
    except ValueError:
        return (len(PRIMITIVE_ORDER), kind)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot KeySwitch execution profile for one method.")
    parser.add_argument("--ks-method", default="poseidon")
    parser.add_argument("--input-log", default="")
    parser.add_argument("--output", default="")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--num-users", type=int, default=1)
    parser.add_argument("--requests-per-user", type=int, default=1)
    parser.add_argument("--inter-arrival", type=int, default=10)
    parser.add_argument("--num-cards", type=int, default=1)
    multi_card_group = parser.add_mutually_exclusive_group()
    multi_card_group.add_argument(
        "--enable-multi-card",
        dest="enable_multi_card",
        action="store_true")
    multi_card_group.add_argument(
        "--disable-multi-card",
        dest="enable_multi_card",
        action="store_false")
    parser.set_defaults(enable_multi_card=False)
    parser.add_argument("--bram-budget-bytes", type=int, default=31876710)
    parser.add_argument("--show", action="store_true")
    return parser.parse_args()

def main():
    args = parse_args()
    plt.style.use('seaborn-v0_8-whitegrid')

    output = ""
    if args.input_log:
        input_path = Path(args.input_log)
        if input_path.exists():
            output = input_path.read_text(encoding="utf-8", errors="replace")
    if not output:
        output = run_sim(args)

    cycles, bram = parse_bram_timeline(output)
    groups = parse_group_timeline(output)
    breakdown = parse_cycle_breakdown(output)
    operation_kind_map = parse_operation_kind_map(output)
    estimated_from_emitted = False

    if len(cycles) == 0 or len(groups) == 0:
        emitted = parse_emitted_group_trace(output)
        if emitted is not None:
            cycles, bram, groups, breakdown = emitted
            estimated_from_emitted = True

    if len(cycles) == 0:
        print("No BRAM timeline data found!")
        sys.exit(1)

    groups = classify_groups_by_primitive(groups, operation_kind_map)
    primitive_breakdown = build_primitive_breakdown(groups)
    if not primitive_breakdown:
        primitive_breakdown = breakdown

    bram_mb = bram / (1024 * 1024)
    total_cycles = max(g['end'] for g in groups) if groups else cycles[-1]

    unique_primitives = sorted(
        {g.get('primitive', 'Other') for g in groups},
        key=primitive_sort_key)
    gantt_rows = max(1, len(unique_primitives))
    fig_height = 9 + 0.35 * gantt_rows
    fig, axes = plt.subplots(
        3,
        1,
        figsize=(18, fig_height),
        height_ratios=[3, max(2.0, 0.35 * gantt_rows), 2.2],
    )
    fig.suptitle(
        f'{args.ks_method.upper()} KeySwitch Execution Profile',
        fontsize=16,
        fontweight='bold')
    if estimated_from_emitted:
        fig.text(
            0.5,
            0.965,
            '(estimated from emitted groups; no detailed simulator timeline)',
            ha='center',
            va='center',
            fontsize=10,
            color='dimgray')

    # --- Plot 1: BRAM Occupancy over time ---
    ax1 = axes[0]
    ax1.fill_between(cycles, bram_mb, alpha=0.3, color='steelblue')
    ax1.plot(cycles, bram_mb, color='steelblue', linewidth=1.5)
    ax1.set_ylabel('BRAM Occupancy (MB)', fontsize=12)
    ax1.set_xlabel('Cycle', fontsize=12)
    ax1.set_title('BRAM Occupancy Over Time', fontsize=13)
    ax1.set_xlim(0, total_cycles)
    ax1.grid(True, alpha=0.3)
    budget_mb = args.bram_budget_bytes / (1024 * 1024)
    ax1.axhline(
        y=budget_mb,
        color='red',
        linestyle='--',
        alpha=0.5,
        label=f'Budget ({budget_mb:.1f} MB)')
    ax1.legend()

    # --- Plot 2: Gantt chart of operations ---
    ax2 = axes[1]
    primitive_to_y = {
        primitive: idx for idx, primitive in enumerate(unique_primitives)
    }

    for g in groups:
        primitive = g.get('primitive', 'Other')
        y = primitive_to_y[primitive]
        color = PRIMITIVE_COLORS.get(primitive, PRIMITIVE_COLORS['Other'])
        ax2.barh(y, g['duration'], left=g['start'], height=0.7,
                 color=color, edgecolor='black', linewidth=0.5, alpha=0.8)

    y_fontsize = 10 if len(unique_primitives) <= 12 else 9
    ax2.set_yticks(range(len(unique_primitives)))
    ax2.set_yticklabels(
        [prettify_label(name) for name in unique_primitives],
        fontsize=y_fontsize)
    ax2.set_xlabel('Cycle', fontsize=12)
    ax2.set_title('Primitive Gantt Chart', fontsize=13)
    ax2.set_xlim(0, total_cycles)
    ax2.grid(True, axis='x', alpha=0.3)
    ax2.invert_yaxis()

    # --- Plot 3: Cycle breakdown donut chart ---
    ax3 = axes[2]
    if primitive_breakdown:
        labels, sizes = aggregate_breakdown_items(
            primitive_breakdown,
            small_pct_threshold=3.0)
        cmap = plt.get_cmap('tab20')
        pie_colors = [cmap(i % cmap.N) for i in range(len(sizes))]
        wedges, _, _ = ax3.pie(
            sizes,
            labels=None,
            autopct=pie_autopct(min_pct=5.0),
            colors=pie_colors,
            startangle=90,
            counterclock=False,
            pctdistance=0.78,
            wedgeprops={'width': 0.45, 'edgecolor': 'white', 'linewidth': 1.0},
            textprops={'fontsize': 9},
        )
        total_breakdown = sum(sizes)
        legend_labels = [
            f'{label}: {cycles:,} cycles ({100.0 * cycles / total_breakdown:.1f}%)'
            for label, cycles in zip(labels, sizes)
        ]
        ax3.legend(
            wedges,
            legend_labels,
            loc='center left',
            bbox_to_anchor=(1.02, 0.5),
            frameon=False,
            title='Operation Type',
            fontsize=9,
            title_fontsize=10,
        )
        ax3.set_title('Cycle Breakdown by Primitive', fontsize=13)
        ax3.set_aspect('equal')
    else:
        ax3.axis('off')
        ax3.text(0.5, 0.5, 'No cycle breakdown data', ha='center', va='center', fontsize=11)

    plt.tight_layout(rect=(0.0, 0.0, 0.88, 0.96))
    out_path = args.output
    if not out_path:
        out_path = f"results/{args.ks_method}_execution_profile.png"
    output_path = Path(out_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(str(output_path), dpi=150, bbox_inches='tight')
    print(f"Saved to {out_path}")
    if args.show:
        plt.show()
    else:
        plt.close(fig)

if __name__ == '__main__':
    main()
