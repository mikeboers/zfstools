import subprocess
import re
import os
import random
import collections

import click


CACHE_ROOT = '/mnt/tank/scratch/zfs-diffs'


OP_REMOVE = '-' # The path has been removed
OP_CREATE = '+' # The path has been created
OP_MODIFY = 'M' # The path has been modified
OP_RENAME = 'R' # The path has been renamed

TYPE_BLK = 'B' # Block device
TYPE_CHR = 'C' # Character device
TYPE_DIR = '/' # Directory
TYPE_DOR = '>' # Door
TYPE_PIP = '|' # Named pipe
TYPE_LNK = '@' # Symbolic link
TYPE_PRT = 'P' # Event port
TYPE_SOC = '=' # Socket
TYPE_REG = 'F' # Regular file


DiffItem = collections.namedtuple('DiffItem', 'relpath time type op path new_relpath')

def decode(x):
    return re.sub(r'\\(\d{4})', lambda m: chr(int(m.group(1), 8)), x)


def iter_diff(volname, snap1, snap2, cache_key=None):

    abs_prefix_len = len(volname) + 6 # /mnt/{volname}/xxx

    for line in _iter_diff(volname, snap1, snap2, cache_key):

        parts = line.rstrip().split('\t')
        time = float(parts[0])
        op = parts[1]
        type_ = parts[2]
        path = decode(parts[3])
        relpath = path[abs_prefix_len:]
        
        if op == 'R':
            new_relpath = decode(parts[4])[abs_prefix_len:]
        else:
            new_relpath = None

        yield DiffItem(relpath, time, type_, op, path, new_relpath)


def _iter_diff(volname, snap1, snap2, cache_key):

    cache_dir = os.path.join(CACHE_ROOT, volname)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    cache_path = os.path.join(cache_dir, f'{snap1},{snap2}{"," if cache_key else ""}{cache_key or ""}.zfsdiff')

    if os.path.exists(cache_path):
        click.echo(f"Loading ZFS diff from cache: {cache_path}")
        with open(cache_path, 'r') as fh:
            yield from fh
        return

    tmp_path = f'{cache_path}.{random.random()}'
    with open(tmp_path, 'w') as fh:
        cmd = ['zfs', 'diff', '-tFH', f'{volname}@{snap1}', f'{volname}@{snap2}']
        click.secho(f"Pulling ZFS diff for first time\n    {' '.join(cmd)}", fg='yellow')
        proc = subprocess.Popen(cmd, bufsize=1, stdout=subprocess.PIPE)
        for line in proc.stdout:
            line = line.decode()
            fh.write(line)
            yield line

    ret = proc.wait()
    if ret:
        click.secho(f"WARNING: zfs diff returned {ret}", fg='yellow')
    if not ret:
        os.rename(tmp_path, cache_path)


if __name__ == '__main__':

    import sys

    for x in iter_diff(*sys.argv[1:]):
        print(x)

