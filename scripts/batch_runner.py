#!/usr/bin/env python3

import argparse
import itertools
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List


def parse_list(value: str, cast):
    items = []
    for part in value.split(','):
        token = part.strip()
        if not token:
            continue
        items.append(cast(token))
    return items


def parse_bool_list(value: str) -> List[bool]:
    mapping = {
        '1': True,
        'true': True,
        'yes': True,
        'on': True,
        '0': False,
        'false': False,
        'no': False,
        'off': False,
    }
    out = []
    for part in value.split(','):
        token = part.strip().lower()
        if not token:
            continue
        if token not in mapping:
            raise ValueError(f'invalid bool token: {part}')
        out.append(mapping[token])
    return out


def parse_args():
    parser = argparse.ArgumentParser(
        description='KeyAwareSwitch batch experiment runner (Step 14).')

    parser.add_argument('--exe', default='./build-cmake/keyaware_sim')
    parser.add_argument('--output-csv', required=True)
    parser.add_argument('--snapshot-out', default='')
    parser.add_argument(
        '--template',
        choices=['custom', 'latency_vs_scheduler', 'latency_vs_load', 'fairness_vs_scheduler'],
        default='custom')
    parser.add_argument('--append-existing', action='store_true')

    parser.add_argument('--scheduler-list', default='fifo')
    parser.add_argument('--backend-list', default='analytical')
    parser.add_argument('--workload-list', default='synthetic')
    parser.add_argument('--seed-list', default='42')
    parser.add_argument('--num-users-list', default='8')
    parser.add_argument('--num-cards-list', default='8')
    parser.add_argument('--multi-card-list', default='true')

    parser.add_argument('--inter-arrival-list', default='300')
    parser.add_argument('--burst-level-list', default='1')

    parser.add_argument('--requests-per-user', type=int, default=4)
    parser.add_argument('--bursts', type=int, default=4)
    parser.add_argument('--requests-per-user-per-burst', type=int, default=2)
    parser.add_argument('--intra-burst-gap', type=int, default=20)
    parser.add_argument('--inter-burst-gap', type=int, default=1200)

    parser.add_argument('--pool-config', default='')
    parser.add_argument('--tree-config', default='')
    parser.add_argument('--profile-table', default='')

    parser.add_argument('--dry-run', action='store_true')

    return parser.parse_args()


def template_dimensions(template: str) -> Dict[str, List]:
    if template == 'latency_vs_scheduler':
        return {
            'schedulers': ['fifo', 'affinity', 'score', 'hierarchical_d'],
            'backends': ['analytical'],
            'workloads': ['synthetic'],
            'seeds': [42, 43],
            'num_users': [8],
            'num_cards': [8],
            'multi_card': [True],
            'inter_arrival': [300],
            'burst_level': [1],
        }

    if template == 'latency_vs_load':
        return {
            'schedulers': ['affinity'],
            'backends': ['table'],
            'workloads': ['synthetic'],
            'seeds': [42],
            'num_users': [8],
            'num_cards': [8],
            'multi_card': [True],
            'inter_arrival': [80, 150, 300, 600],
            'burst_level': [1],
        }

    if template == 'fairness_vs_scheduler':
        return {
            'schedulers': ['fifo', 'affinity', 'score', 'hierarchical_d'],
            'backends': ['table'],
            'workloads': ['burst'],
            'seeds': [42],
            'num_users': [8],
            'num_cards': [8],
            'multi_card': [True],
            'inter_arrival': [300],
            'burst_level': [2],
        }

    return {}


def build_dimensions_from_args(args) -> Dict[str, List]:
    if args.template != 'custom':
        return template_dimensions(args.template)

    return {
        'schedulers': parse_list(args.scheduler_list, str),
        'backends': parse_list(args.backend_list, str),
        'workloads': parse_list(args.workload_list, str),
        'seeds': parse_list(args.seed_list, int),
        'num_users': parse_list(args.num_users_list, int),
        'num_cards': parse_list(args.num_cards_list, int),
        'multi_card': parse_bool_list(args.multi_card_list),
        'inter_arrival': parse_list(args.inter_arrival_list, int),
        'burst_level': parse_list(args.burst_level_list, int),
    }


def snapshot_payload(args, dimensions: Dict[str, List], run_count: int):
    return {
        'created_at_utc': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
        'template': args.template,
        'executable': args.exe,
        'output_csv': args.output_csv,
        'run_count': run_count,
        'dimensions': dimensions,
        'fixed_workload_params': {
            'requests_per_user': args.requests_per_user,
            'bursts': args.bursts,
            'requests_per_user_per_burst': args.requests_per_user_per_burst,
            'intra_burst_gap': args.intra_burst_gap,
            'inter_burst_gap': args.inter_burst_gap,
        },
        'config_sources': {
            'pool_config': {
                'path': args.pool_config if args.pool_config else 'built-in default',
                'mode': 'external' if args.pool_config else 'built-in',
            },
            'tree_config': {
                'path': args.tree_config if args.tree_config else 'built-in default',
                'mode': 'external' if args.tree_config else 'built-in',
            },
            'profile_table': {
                'path': args.profile_table if args.profile_table else 'built-in default',
                'mode': 'external' if args.profile_table else 'built-in',
            },
        },
    }


def build_run_commands(args, dims: Dict[str, List]) -> Iterable[List[str]]:
    keys = [
        'schedulers',
        'backends',
        'workloads',
        'seeds',
        'num_users',
        'num_cards',
        'multi_card',
        'inter_arrival',
        'burst_level',
    ]

    for values in itertools.product(*(dims[k] for k in keys)):
        (
            scheduler,
            backend,
            workload,
            seed,
            num_users,
            num_cards,
            multi_card,
            inter_arrival,
            burst_level,
        ) = values

        cmd = [
            args.exe,
            '--scheduler', str(scheduler),
            '--backend', str(backend),
            '--workload', str(workload),
            '--seed', str(seed),
            '--num-users', str(num_users),
            '--num-cards', str(num_cards),
            '--requests-per-user', str(args.requests_per_user),
            '--inter-arrival', str(inter_arrival),
            '--bursts', str(args.bursts),
            '--requests-per-user-per-burst', str(args.requests_per_user_per_burst),
            '--intra-burst-gap', str(args.intra_burst_gap),
            '--inter-burst-gap', str(args.inter_burst_gap),
            '--burst-level', str(burst_level),
            '--csv-output', args.output_csv,
        ]

        if multi_card:
            cmd.append('--enable-multi-card')
        else:
            cmd.append('--disable-multi-card')

        if args.pool_config:
            cmd.extend(['--pool-config', args.pool_config])
        if args.tree_config:
            cmd.extend(['--tree-config', args.tree_config])
        if args.profile_table:
            cmd.extend(['--profile-table', args.profile_table])

        yield cmd


def main():
    args = parse_args()

    dims = build_dimensions_from_args(args)
    for name, values in dims.items():
        if not values:
            print(f'Error: empty sweep dimension: {name}', file=sys.stderr)
            return 1

    output_csv = Path(args.output_csv)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    if output_csv.exists() and not args.append_existing:
        output_csv.unlink()

    commands = list(build_run_commands(args, dims))

    snapshot_path = Path(args.snapshot_out) if args.snapshot_out else output_csv.with_suffix('.snapshot.json')
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    with snapshot_path.open('w', encoding='utf-8') as f:
        json.dump(snapshot_payload(args, dims, len(commands)), f, indent=2)

    print(f'[batch] template={args.template} runs={len(commands)} output={output_csv}')
    print(f'[batch] snapshot={snapshot_path}')

    for idx, cmd in enumerate(commands, start=1):
        print(f'[batch] ({idx}/{len(commands)})', ' '.join(cmd))
        if args.dry_run:
            continue
        completed = subprocess.run(cmd, check=False)
        if completed.returncode != 0:
            print(f'[batch] failed at run {idx}', file=sys.stderr)
            return completed.returncode

    print('[batch] done')
    return 0


if __name__ == '__main__':
    sys.exit(main())
