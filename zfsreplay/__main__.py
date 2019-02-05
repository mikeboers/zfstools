
'''



There was a big re-org around 2016-10-16T18-24-34
    this was the introduction of the "trailer" directory
    otherwise there are NO moves in the rsync based backup (as there wouldn't be. bah)

TODO:
    - Do we re-org things as we go?
        This would be one way to be efficient-ish.
    - Do we hash everything as we go?
        This would be the most efficient, but a bit of work for me.

'''


import argparse
import collections
import datetime as dt
import os
import stat
import subprocess

from .index import Index



Snapshot = collections.namedtuple('Snapshot', ('fullname', 'volname', 'name', 'creation', 'root'))

def get_snapshots(volume):

    snaps_root = os.path.join('/mnt', volume, '.zfs', 'snapshot')

    snapshots = []

    output = subprocess.check_output(['zfs', 'list', '-rd1', '-tsnap', '-Hp', '-oname,creation', volume])
    for line in output.decode().splitlines():

        fullname, creation_raw = line.strip().split('\t')
        volname, name = fullname.split('@')
        creation = dt.datetime.fromtimestamp(int(creation_raw))

        snapshots.append(Snapshot(fullname, volname, name, creation, os.path.join(snaps_root, name)))

    return snapshots


class Job(object):

    def __init__(self, volname, snapname, target=None, sort_key=None):
        self.volname = volname
        self.snapname = snapname
        self.target = target or os.path.join('/mnt', volname)
        self.sort_key = sort_key or snapname


class SyncJob(Job):

    def __init__(self, volname, snapname, a, b, target=None, sort_key=None, ignore=None, is_zfs=False):
        super().__init__(volname, snapname, target, sort_key)
        self.a = a
        self.b = b
        self.ignore = ignore
        self.is_zfs = is_zfs

    def __repr__(self):
        return (
            f'Sync: {self.volname:20s}@{self.snapname:24s} target={self.target:40s} as a={self.a:80s} to b={self.b:80s} '
            f'{"is_zfs" if self.is_zfs else ""}'
        )

    def run(self, dry_run=False):

        self.dry_run = dry_run

        ''' 1. Get a full index of A and B. Assume T starts looking like A.
            - This is the paths and stats of all folders and files. Folders don't need their contents.
        '''
        aidx = Index.get(self.a, ignore=self.ignore)
        bidx = Index.get(self.b, ignore=self.ignore)
        
        '''3. Identify all AB pairs; this will be via inode and then name.
            - Same inode from/to link snapshot means the file has not changed.
            - Same ctime from/to zfs snapshot means the file has not changed.
            - Same indirect block from/to zfs snapshot means the file has not changed.
                This is kinda slow, so only bother when the file is large. I don't think
                we can make it go much faster without a TON of effort.
        '''
        a_by_rel = aidx.by_rel.copy()
        b_by_rel = bidx.by_rel.copy()
        pairs = []

        # Collect pairs by inode.
        for inode, bnodes in bidx.by_ino.items():

            anodes = aidx.by_ino.get(inode, ())
            if not anodes:
                continue

            # We have them by inodes, so we don't need to look at them by path.
            for n in anodes:
                a_by_rel.pop(n.relpath)
            for n in bnodes:
                b_by_rel.pop(n.relpath)

            # If we have multiple, treat them all as a[0] to all bs. This will
            # work out fine. Promise.
            for n in bnodes:
                pairs.append((anodes[0], n))

        # Collect pairs by relpath.
        for relpath, b in list(b_by_rel.items()):

            a = a_by_rel.get(relpath)

            # Only counts as a pair if they are the same type.
            if a is None or a.fmt != b.fmt:
                continue
            
            del a_by_rel[relpath]
            del b_by_rel[relpath]

            pairs.append((a, b))

        '''4. Iterate across AB pairs:
            - Move files that don't have matching paths.
            - Scan/update if there may have been changes.
                - Iterate in either 16KiB or 128KiB chunks.
                - If they are the same, keep going.
                - If they are different, seek back, and write the new block.
                - If after 10 blocks more than 90% of blocks have changed, assume it
                  has fully changed.
            - Update uid/gid/perms.
        '''
        for a, b in pairs:
            
            bpath = b.path
            relpath = b.relpath
            tpath = os.path.join(self.target, relpath)

            if a.relpath != relpath:
                self.rename(os.path.join(self.target, a.relpath), tpath)

            # If this is not ZFS, hardlinks can't have different (meta)data.
            if (not self.is_zfs) and a.stat.st_ino == b.stat.st_ino:
                continue

            # If this is ZFS, files with same ctime can't have been modified.
            if self.is_zfs and a.stat.st_ctime == b.stat.st_ctime:
                continue

            if b.is_dir:
                # We're not doing anything; this is just for control flow.
                pass

            elif b.is_lnk:
                # Just assume it didn't change.
                # TODO DO THIS
                pass

            # If they're different sizes, lets just assume they are different.
            elif a.stat.st_size != b.stat.st_size:
                self.copy(bpath, tpath)

            else:
                self.merge(bpath, tpath)

            if a.stat.st_mode != b.stat.st_mode:
                self.chmod(tpath, b.stat.st_mode)

            if (a.stat.st_uid != b.stat.st_uid) or (a.stat.st_gid != b.stat.st_gid):
                self.chown(tpath, b.stat.st_uid, b.stat_st_gid)

            # Times will almost always need to be set at this point.
            if not self.dry_run:
                self.utime(tpath, b.stat.st_atime, b.stat.st_mtime)


        # 5. Delete all files and directories that are in A but not B.
        # We're going in reverse so files are done before directories.
        for relpath, node in sorted(a_by_rel.items(), reverse=True):
            tpath = os.path.join(self.target, relpath)
            if node.is_dir:
                self.rmdir(tpath)
            else:
                self.unlink(tpath)

        #6. Create missing dirs/files.
        for relpath, node in b_by_rel.items():

            bpath = node.path
            tpath = os.path.join(self.target, relpath)

            if node.is_dir:
                self.mkdir(tpath)

            elif node.is_lnk:
                # TODO: DO THIS
                continue

            else:
                self.copy(bpath, tpath)

            if not node.is_lnk:
                self.chmod(tpath, b.stat.st_mode)
            self.chown(tpath, b.stat.st_uid, b.stat.st_gid)
            self.utime(tpath, b.stat.st_atime, b.stat.st_mtime)

    def rename(self, src, dst):
        print('rename', src, dst)
        if not self.dry_run:
            os.rename(src, dst)

    def rmdir(self, path):
        print('rmdir', path)
        if not self.dry_run:
            os.rmdir(path)

    def unlink(self, path):
        print('unlink', path)
        if not self.dry_run:
            os.unlink(path)

    def mkdir(self, path):
        print('mkdir', path)
        if not self.dry_run:
            os.mkdir(path)

    def chmod(self, path, mode):
        print('chmod', mode, path)
        if not self.dry_run:
            os.chmod(path, mode) #, follow_symlinks=False)

    def chown(self, path, uid, gid):
        print(f'chown {uid}:{gid} {path}')
        if not self.dry_run:
            os.chown(path, uid, gid, follow_symlinks=False)

    def utime(self, path, atime, mtime):
        print(f'utime {atime}:{mtime} {path}')
        if not self.dry_run:
            os.utime(path, (atime, mtime), follow_symlinks=False)

    def copy(self, src_path, dst_path):

        print('copy', src_path, dst_path)
        if self.dry_run:
            return

        # This is the large end of our ZFS block sizes.
        size = 128 * 1024

        with open(src_path, 'rb') as src, open(dst_path, 'wb') as dst:
            while True:
                chunk = src.read(size)
                if not chunk:
                    break
                dst.write(chunk)

    def merge(self, src_path, dst_path):

        print('merge', src_path, dst_path)
        if not self.dry_run:
            return

        size = 128 * 1024
        n_diff = 0

        with open(src_path, 'rb') as src, open(dst_path, 'r+b') as dst:

            # Keep only writing changes until we've passed 3 blocks that
            # are different.
            while n_diff < 3:

                pos = dst.tell()

                a = src.read(size)
                b = dst.read(size)

                if len(a) != len(b):
                    raise ValueError(f"Read len mismatch at {pos}: {len(a)} != {len(b)}")

                # We're done.
                if not a:
                    break

                # The blocks match; don't do anything.
                if a == b:
                    continue

                dst.seek(pos)
                dst.write(a)

                n_diff += 1

            # We gave up comparing; just finish it up normally.
            while a:
                a = src.read(size)
                if a:
                    dst.write(a)







jobs = []



def make_jobs(snaps, dst_volume, src_subdir='', dst_subdir='', ignore=None, skip_start=False, is_zfs=False):

    target = os.path.normpath(os.path.join('/mnt', dst_volume, dst_subdir))

    for i in range(len(snaps) - 1):

        a = snaps[i]
        b = snaps[i + 1]

        if (not i) and (not skip_start):
            jobs.append(SyncJob(
                dst_volume,
                a.creation.strftime('%Y-%m-%dT%H') + '.' + a.volname.split('/')[-1],
                a=target,
                b=os.path.normpath(os.path.join(a.root, src_subdir)),
                target=target,
            ))

        jobs.append(SyncJob(
            dst_volume,
            b.creation.strftime('%Y-%m-%dT%H') + '.' + b.volname.split('/')[-1],
            a=os.path.normpath(os.path.join(a.root, src_subdir)),
            b=os.path.normpath(os.path.join(b.root, src_subdir)),
            target=target,
            is_zfs=is_zfs,
        ))


def make_zfs_jobs(src_volume, *args, **kwargs):
    snaps = get_snapshots(src_volume)

    make_jobs(snaps, *args, **kwargs, is_zfs=True)


def make_timestamped_jobs(src_name, *args, **kwargs):

    snaps = []

    root = os.path.join('/mnt/tank/heap/sitg/backups', src_name)
    for name in sorted(os.listdir(root)):

        if not name.startswith('20'):
            continue

        try:
            creation = dt.datetime.strptime(name, '%Y-%m-%dT%H-%M-%S')
        except ValueError:
            creation = dt.datetime.strptime(name, '%Y-%m-%d')


        snaps.append(Snapshot(
            f'backups/{src_name}@{name}',
            'bak',
            name,
            creation,
            os.path.join(root, name),
        ))

    make_jobs(snaps, *args, **kwargs)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--dry-run', action='store_true')
    parser.add_argument('set')
    args = parser.parse_args()

    # self.dry_run = True


    if args.set == 'main':

        make_timestamped_jobs('work', 'tank/sitg',
            dst_subdir='work',
        )

        make_zfs_jobs('tank/heap/sitg', 'tank/sitg',
            ignore=set(('backup', 'cache', 'out', 'work')),
            skip_start=True,
        )

        make_zfs_jobs('tank/heap/sitg/work', 'tank/sitg',
            dst_subdir='work',
            ignore=set(('artifacts-film', )),
        )



    if args.set == 'artifacts':

        make_timestamped_jobs('out', 'tank/sitg/artifacts',
            dst_subdir='trailer',
        )

        make_zfs_jobs('tank/heap/sitg/work', 'tank/sitg/artifacts',
            src_subdir='artifacts-film',
            ignore=set(('trailer', )),
        )


    if args.set == 'cache':

        # Seed the caches. We only have the one set from the trailer.
        jobs.append(SyncJob(
            'tank/sitg/cache',
            '2015-08-02T00.sitg',
            target='/mnt/tank/sitg/cache/TE',
            a='/mnt/tank/sitg/cache/TE',
            b='/mnt/tank/heap/sitg/cache',
        ))

        # The work caches.
        make_zfs_jobs('tank/heap/sitg/work', 'tank/sitg/cache',
            src_subdir='cache-film',
            ignore=set(('TE', )),
        )


    jobs.sort(key=lambda j: j.sort_key)

    assert len(set(j.volname for j in jobs)) == 1


    snaps = get_snapshots(jobs[0].volname)

    cmd = ['zfs', 'rollback', snaps[-1].fullname]
    print(' '.join(cmd))
    if not args.dry_run:
        subprocess.check_call(cmd)


    for job in jobs:

        if any(s.name == job.snapname for s in snaps):
            continue

        print(job)
        job.run(dry_run=args.dry_run)

        cmd = ['zfs', 'snapshot', f'{job.volname}@{job.snapname}']
        print(' '.join(cmd))
        if not args.dry_run:
            subprocess.check_call(cmd)

        break


