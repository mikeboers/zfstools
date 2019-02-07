import collections
import datetime as dt
import os
import subprocess


Snapshot = collections.namedtuple('Snapshot', ('name', 'volume', 'snapname', 'creation', 'root'))


def get_snapshots(volume):

    res = []

    snaproot = None

    output = subprocess.check_output(['zfs', 'list', '-rd1', '-tall', '-Hp', '-otype,name,creation,mountpoint', volume])
    for line in output.decode().splitlines():

        type_, name, creation_raw, mountpoint = line.strip().split('\t')

        if type_ == 'filesystem':
            # If there is a child filesystem the above command will list it
            # as well. Just ignore it.
            if not snaproot:
                snaproot = os.path.join(mountpoint, '.zfs', 'snapshot')
            continue

        snapvol, snapname = name.split('@')
        if snapvol != volume:
            continue

        creation = dt.datetime.fromtimestamp(int(creation_raw))

        res.append(Snapshot(name, volume, snapname, creation, os.path.join(snaproot, snapname)))

    return res

