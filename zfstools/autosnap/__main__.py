#!/usr/bin/env python

from __future__ import print_function

import argparse
import datetime
import os
import re
import subprocess
import sys
import time

from .schedule import label_snapshots, parse_datetime


IGNORE_NAMES = set('''
    .DS_Store
'''.strip().split())


def main():


    parser = argparse.ArgumentParser()

    parser.add_argument('-i', '--include', action='append', default=[])
    parser.add_argument('-e', '--exclude', action='append', default=[])

    parser.add_argument('-n', '--dry-run', action='store_true')
    parser.add_argument('-y', '--yes', action='store_true')

    parser.add_argument('-v', '--verbose', action='count')

    parser.add_argument('command', choices=['auto', 'snapshot', 'prune', 'prune-timed', 'prune-empty'])
    parser.add_argument('volumes', nargs='*')

    args = parser.parse_args()


    timestamp = time.strftime('%Y-%m-%dT%H:%M:%S%z')


    do_snapshot = args.command in ('auto', 'snapshot')
    do_prune_timed = args.command in ('auto', 'prune', 'prune-timed')
    do_prune_empty = args.command in ('auto', 'prune', 'prune-empty')


    def destroy(snapshot):
        cmd = ['sudo', 'zfs', 'destroy', snapshot]
        if args.verbose:
            print('    $', ' '.join(cmd))
        if not args.dry_run:
            yes = args.yes
            if not yes:
                res = raw_input('Delete %s ? [yN]: ' % snapshot).strip()
                yes = res.lower() in ('y', 'yes')
            if yes:
                subprocess.check_call(cmd)


    code = 0

    for line in subprocess.check_output(['sudo', 'zfs', 'list', '-H', '-o', 'name,autosnap:autosnap,autosnap:autoprune']).splitlines():
        
        line = line.strip().decode()
        if not line:
            continue

        volume, is_auto_snap, is_auto_prune = line.strip().split('\t')

        is_auto_snap  = is_auto_snap.upper() in ('1', 'Y', 'YES', 'T', 'TRUE', 'ON')
        is_auto_prune = is_auto_prune.upper() in ('1', 'Y', 'YES', 'T', 'TRUE', 'ON')

        do_this_snap = is_auto_snap
        do_this_prune = do_this_snap or is_auto_prune

        if args.volumes:
            if volume in args.volumes:
                do_this_snap = do_this_prune = True
            else:
                continue

        if volume in args.include:
            do_this_snap = do_this_prune = True

        if volume in args.exclude:
            if args.verbose:
                print("Skipping excluded", volume, ".")
            continue

        if not (do_this_snap or do_this_prune):
            continue
        
        if args.verbose:
            print()
            print(volume)
            print('=' * 20)

        if do_this_snap and do_snapshot:
            cmd = ['sudo', 'zfs', 'snapshot', '{}@{}'.format(volume, timestamp)]
            if args.verbose > 1:
                print('$', ' '.join(cmd))
            if not args.dry_run:
                this_code = subprocess.call(cmd)
                code = code or this_code
                if this_code:
                    print('ERROR: Non-zero return code {} from: {}'.format(this_code, ' '.join(cmd)))
                    continue

        if not do_this_prune:
            continue
        if not (do_prune_timed or do_prune_empty):
            continue

        snapshots = []

        for line in subprocess.check_output(['sudo', 'zfs', 'list',
            '-t', 'snapshot',
            '-r', '-d', '1',
            '-Hp', '-o', 'name,used,autosnap:maxage,autosnap:_nonempty',
            volume,
        ]).splitlines():
            line = line.strip().decode()
            if not line:
                continue
            snapshot, used, maxage, nonempty = (x.strip() for x in line.split('\t'))
            try:
                maxage = None if maxage == '-' else int(maxage)
            except ValueError:
                print(f"Snapshot {snapshot} has malformed maxage {maxage!r}")
                maxage = None
            raw_ctime = snapshot.split('@', 1)[1]
            ctime = parse_datetime(raw_ctime)
            if not ctime:
                if args.verbose > 1:
                    print('Could not parse:', snapshot)
                continue
            snapshots.append((snapshot, ctime, maxage, int(used), nonempty))

        snapshots.sort()

        if do_prune_empty:
            # Skip the last, and go backwards so we can pop them out
            # of the master list.
            to_check = reversed(list(enumerate(snapshots))[:-1])
            for i, (snapshot, raw_ctime, maxage, used, nonempty) in to_check:

                if args.verbose > 1:
                    print('%s -> %s' % (snapshot, used))

                # Obviously too large.
                if used > 1024**2: #102400: # 100kB.
                    continue

                # We've already checked it.
                if nonempty == '1':
                    continue

                if used:

                    cmd = ['sudo', 'zfs', 'diff', '-FH', snapshot, snapshots[i + 1][0]]
                    if args.verbose:
                        print('    $', ' '.join(cmd))
                    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

                    empty = True
                    for line in proc.stdout:

                        line = line.strip().decode()
                        if not line:
                            continue

                        line_parts = line.split('\t')
                        op = line_parts[0]
                        type_ = line_parts[1]
                        path = line_parts[2]

                        # Don't care about metadata on directories.
                        if op == 'M' and type_ == '/':
                            continue

                        # Don't care about a few little files.
                        name = os.path.basename(path)
                        if name in IGNORE_NAMES:
                            continue

                        if args.verbose:
                            print('      Found change:', path)
                        empty = False
                        break

                    if not empty:
                        cmd = ['sudo', 'zfs', 'set', 'autosnap:_nonempty=1', snapshot]
                        if args.verbose > 1:
                            print('$', ' '.join(cmd))
                        subprocess.check_call(cmd)
                        continue

                # At this point they are empty!
                destroy(snapshot)
                snapshots.pop(i)

        labels = label_snapshots([x[:3] for x in snapshots])

        for snapshot, raw_ctime, maxage, used, nonempty in sorted(snapshots):
            label = labels.get(snapshot)
            if args.verbose:
                print(f'{label or "-":7s} {snapshot}')
            if not label:
                destroy(snapshot)

        if args.verbose:
            print()
        
 
main()

