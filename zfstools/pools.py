#!/usr/bin/env python

import subprocess
import collections
import re


Pool  = collections.namedtuple('Pool', 'name stats vdevs')
Vdev  = collections.namedtuple('Vdev', 'name stats index disks')
Disk  = collections.namedtuple('Disk', 'name index')
Stats = collections.namedtuple('Stats', 'NAME SIZE ALLOC FREE EXPANDSZ FRAG CAP DEDUP HEALTH ALTROOT'.lower())

vdev_types = set('''
    raidz1
    raidz2
    raidz3
    log
    cache
    spare
    mirror
'''.strip().split())

zpool_list_command = 'zpool list -v'

class _ZPoolListParser(object):

    def __init__(self, input_=None):
        self.pools = []
        self._pool = None
        self._vdevs = []
        self._vdev = None
        self._disks = []
        self.parse(input_)

    def parse(self, input_=None):

        input_ = input_ or subprocess.check_output(['zpool', 'list', '-v']).decode()
        lines = input_.splitlines()
        lines.pop(0) # Headers.

        for line in lines:

            parts = line.strip().split()
            parts = [None if x == '-' else x for x in parts]
            while len(parts) < 10:
                parts.append(None)
            stats = Stats(*parts)

            indent = len(line) - len(line.lstrip())
            is_pool = bool(stats.health)
            is_vdev = stats.name in vdev_types
            is_disk = not (is_pool or is_vdev)

            # So... cache/spare/log/etc. may not be per-pool.
            
            # print '%s%s%s %s' % (
            #     ' P'[is_pool],
            #     ' V'[is_vdev],
            #     ' D'[is_disk],
            #     stats,
            # )

            if is_disk:
                self._disks.append(Disk(parts[0], len(self._disks)))
                continue

            if is_vdev:
                self.finish_vdev()
                self._vdev = stats
                continue

            if is_pool:
                self.finish_pool()
                self._pool = stats

        self.finish_pool()

    def finish_vdev(self):
        if not self._vdev:
            return
        vdev = Vdev(self._vdev.name, self._vdev, len(self._vdevs), tuple(self._disks))
        self._vdevs.append(vdev)
        self._vdev = None
        self._disks = []

    def finish_pool(self):
        if not self._pool:
            return
        self.finish_vdev()
        pool = Pool(self._pool.name, self._pool, tuple(self._vdevs))
        self.pools.append(pool)
        self._pool = None
        self._vdevs = []


def list_pools(input_=None):
    return _ZPoolListParser(input_).pools




def iter_generic_stats(name):
    with open(os.path.join(ROOT, name), 'rb') as fh:
        fh.next() # Skip pre-header.
        fh.next() # Skip header.
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            name = parts[0]
            value = parts[-1]
            yield name, int(value)

def iter_pool_stats(name):
    with open(os.path.join(ROOT, name, 'io'), 'rb') as fh:
        fh.next() # Skip the header.
        names = fh.next().strip().split()
        values = map(int, fh.next().strip().split())
        return zip(names, values)

def iter_pools():
    for name in os.listdir(ROOT):
        path = os.path.join(ROOT, name, 'io')
        if os.path.exists(path):
            yield name

def iter_all_stats():
    """Iter 3-tuples for :func:`mmsystems.influx.format_line`."""
    yield 'zfs.arc', {}, iter_generic_stats('/proc/spl/kstat/zfs/arcstats')
    yield 'zfs.zil', {}, ((name[4:], value) for name, value in iter_generic_stats('/proc/spl/kstat/zfs/zil'))
    for pool in iter_pools():
        yield 'zfs.io', {'pool': pool}, iter_pool_stats(pool)



if __name__ == '__main__':

    for pool in list_pools():
        print(pool)
        for vdev in pool.vdevs:
            print(vdev)
            for disk in vdev.disks:
                print(disk)





