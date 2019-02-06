
from concurrent import futures
import argparse
import collections
import datetime as dt
import os
import pdb
import random
import re
import shutil
import stat
import subprocess

import click

from .index import Index
from .processor import Processor


Snapshot = collections.namedtuple('Snapshot', ('fullname', 'volname', 'name', 'creation', 'root'))

def get_zfs_snapshots(volume):

    snaps_root = os.path.join('/mnt', volume, '.zfs', 'snapshot')

    snapshots = []

    output = subprocess.check_output(['zfs', 'list', '-rd1', '-tsnap', '-Hp', '-oname,creation', volume])
    for line in output.decode().splitlines():

        fullname, creation_raw = line.strip().split('\t')
        volname, name = fullname.split('@')
        creation = dt.datetime.fromtimestamp(int(creation_raw))

        snapshots.append(Snapshot(fullname, volname, name, creation, os.path.join(snaps_root, name)))

    return snapshots

def get_zfs_volume(path):
    m = re.match(r'^/mnt/(.+?)/\.zfs', path)
    if m:
        return m.group(1)


def get_zfs_block(volume, object):

    ret = None

    proc = subprocess.Popen(['zdb', '-ddddd', volume, str(object)], stdout=subprocess.PIPE)
    for line in proc.stdout:

        # Looks like:
        #    0 L5      0:29ed4bcd7000:3000 20000L/1000P F=14 B=14229055/14229055
        m = re.match(rb'\s*0\s+L\d\s+([0-9a-f]+:[0-9a-f]+:[0-9a-f]+)', line)
        if not m:
            continue
        
        ret = m.group(1)
        proc.stdout.close()
        break

    proc.terminate()
    proc.kill()

    return ret


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

        self._prename_root = os.path.join(target, f'.zfsreplay-{random.randrange(1e12)}')
        self._prename_dir = None
        self._prename_count = 0
        self._prename_group = None

    def run(self, proc, threads=1):

        self._proc = proc

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
        # For linked snapshots we can directly see when files have not been changed.
        # For ZFS snapshots, we get 99% information into if they have been moved.
        for inode, bnodes in bidx.by_ino.items():

            # We only deal with files like this.
            if not bnodes[0].is_file:
                continue

            anodes = aidx.by_ino.get(inode, ())
            if not anodes:
                continue

            # We need to be careful because inodes can be recycled between ZFS
            # snapshots.

            # We still aren't really sure what is going on.
            # These two checks aren't quite perfect, but... eh.
            if not anodes[0].is_file:
                click.secho('WARNING: inode match despite file type', fg='yellow')
                click.secho('\n'.join(map(str, anodes)), fg='yellow')
                click.secho('\n'.join(map(str, bnodes)), fg='yellow')
                continue

            if (
                anodes[0].stat.st_size != bnodes[0].stat.st_size and
                os.path.basename(anodes[0].relpath) != os.path.basename(bnodes[0].relpath)
            ):
                click.secho('WARNING: inode appears recycled:', fg='yellow')
                click.secho('\n'.join(map(str, anodes)), fg='yellow')
                click.secho('\n'.join(map(str, bnodes)), fg='yellow')
                continue

            # We have them by inodes, so we don't need to look at them by path.
            for n in anodes:
                a_by_rel.pop(n.relpath)
            for n in bnodes:
                b_by_rel.pop(n.relpath)

            if len(anodes) > 1 or len(bnodes) > 1:
                click.secho("WARNING: There are hardlinks.", fg='yellow')
                click.secho('\n'.join(map(str, anodes)), fg='yellow')
                click.secho('\n'.join(map(str, bnodes)), fg='yellow')

            # If we have multiple, treat them all as a[0] to all bs. This will
            # work out fine. Promise.
            #for n in bnodes:
            #    pairs.append((anodes[0], n))

            pairs.append((anodes[0], bnodes[0]))

        # Collect pairs by relpath.
        for relpath, b in list(b_by_rel.items()):

            a = a_by_rel.get(relpath)

            # Only counts as a pair if they are the same type.
            if a is None or a.fmt != b.fmt:
                continue
            
            del a_by_rel[relpath]
            del b_by_rel[relpath]

            pairs.append((a, b))


        # We MUST operate in this order:
        # - pre-move moving files aside
        # - remove old dirs/files
        # - create new dirs
        # - create new files and update existing
        # - set mtime on dirs


        # Pre-move moving files.
        # This gets them out of directories that are going to be removed, and
        # out of the way of other files or directories that might go in their
        # place. It is a bit of overhead to do it for all of them, but whatever.
        for a, b in pairs:
            if a.is_file and a.relpath != b.relpath:
                count = self._prename_count
                self._prename_count = count + 1
                group, node = divmod(count, 256)
                if group != self._prename_group:
                    self._prename_group = group
                    self._prename_dir = os.path.join(self._prename_root, f'{group:02x}')
                    os.makedirs(self._prename_dir)
                b.prename_path = os.path.join(self._prename_dir, f'{node:02x}')
                proc.prename(os.path.join(self.target, a.relpath), b.prename_path)

        # Delete all files and directories that are in A but not B.
        # We're going in reverse so files are done before directories.
        for relpath, node in sorted(a_by_rel.items(), reverse=True):
            tpath = os.path.join(self.target, node.relpath)
            if node.is_dir:
                self._proc.rmdir(tpath)
            else:
                self._proc.unlink(tpath)

        # Create new directories.
        # Their mtimes will be set wrong if there are any contents added, so
        # we will defer that to later.
        for node in b_by_rel.values():
            if node.is_dir:
                self.create_new(node, utime=False)

        work = []

        # Update files which exist in both.
        # This will be the secondary moves.
        work.extend((self.update_pair, (a, b)) for a, b in pairs)

        # Create new dirs/files that are in B but not A.
        work.extend((self.create_new, (node, )) for node in b_by_rel.values() if not node.is_dir)

        # For aesthetics, we do them in order.
        work.sort(key=lambda x: x[1][-1].path)

        executor = futures.ThreadPoolExecutor(threads)
        for _ in executor.map(lambda x: x[0](*x[1]), work):
            pass

        # Cleanup the premove root.
        if self._prename_count:
            shutil.rmtree(self._prename_root)

        # Finally we set the mtimes of all directories.
        # It might be marginally more efficient to track the changes we've
        # made and not hit the filesystem for it. Oh well.
        if not proc.dry_run:
            for b in bidx.nodes:
                if b.is_dir:
                    tpath = os.path.join(self.target, b.relpath)
                    st = os.stat(tpath)
                    if (st.st_atime != b.stat.st_atime) or (st.st_mtime != b.stat.st_mtime):
                        proc.utime(tpath, b.stat.st_atime, b.stat.st_mtime, verbosity=3)

    def update_pair(self, a, b):

        proc = self._proc

        bpath = b.path
        tpath = os.path.join(self.target, b.relpath)

        # Move everything into the target namespace first.
        # They've already been moved.
        if a.is_file and a.relpath != b.relpath:
            proc.rename(b.prename_path, tpath, original=a.relpath)

        # If this is not ZFS, hardlinks can't have different (meta)data.
        if (not self.is_zfs) and a.stat.st_ino == b.stat.st_ino:
            return

        # If this is ZFS, files with same ctime can't have been modified.
        if self.is_zfs and a.stat.st_ctime == b.stat.st_ctime:
            return

        if b.is_dir:
            # We're not doing anything; this is just for control flow.
            pass

        elif b.is_link:
            if a.link_dest != b.link_dest:
                proc.unlink(tpath, verbosity=3)
                proc.symlink(b.link_dest, tpath)

        # If they're different sizes, lets just assume they are different.
        elif a.stat.st_size != b.stat.st_size:
            proc.copy(bpath, tpath)

        # Try to efficiently update them.
        else:

            nochange = False
            if self.is_zfs and b.stat.st_size > (1024 * 1024 * 50):
                avol = get_zfs_volume(a.path)
                bvol = get_zfs_volume(b.path)
                ablock = avol and get_zfs_block(avol, a.path)
                bblock = bvol and get_zfs_block(bvol, b.path)
                if ablock and ablock == bblock:
                    nochange = True
                    if proc.verbose:
                        print(f'{"nochange":10}', tpath)
            
            if not nochange:
                proc.merge(bpath, tpath)

        # Metadata!
        if a.stat.st_mode != b.stat.st_mode:
            proc.chmod(tpath, b.stat.st_mode)
        if (a.stat.st_uid != b.stat.st_uid) or (a.stat.st_gid != b.stat.st_gid):
            proc.chown(tpath, b.stat.st_uid, b.stat.st_gid)

        # Times will almost always need to be set at this point.
        proc.utime(tpath, b.stat.st_atime, b.stat.st_mtime, verbosity=3)

    def create_new(self, b, utime=True):

        proc = self._proc

        bpath = b.path
        tpath = os.path.join(self.target, b.relpath)

        if b.is_dir:
            proc.mkdir(tpath)

        elif b.is_link:
            proc.symlink(b.link_dest, tpath)

        else:
            proc.copy(bpath, tpath)

        if not b.is_link:
            # We just don't have the capability in our Python for some reason,
            # even though it should be availible.
            proc.chmod(tpath, b.stat.st_mode, verbosity=3)

        proc.chown(tpath, b.stat.st_uid, b.stat.st_gid, verbosity=3)
        
        if utime:
            proc.utime(tpath, b.stat.st_atime, b.stat.st_mtime, verbosity=3)



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
                ignore=ignore,
            ))

        jobs.append(SyncJob(
            dst_volume,
            b.creation.strftime('%Y-%m-%dT%H') + '.' + b.volname.split('/')[-1],
            a=os.path.normpath(os.path.join(a.root, src_subdir)),
            b=os.path.normpath(os.path.join(b.root, src_subdir)),
            target=target,
            ignore=ignore,
            is_zfs=is_zfs,
        ))


def make_zfs_jobs(src_volume, dst_volume, src_subdir='', *args, **kwargs):

    snaps = get_zfs_snapshots(src_volume)

    if src_subdir:
        snaps = [s for s in snaps if os.path.exists(os.path.join(s.root, src_subdir))]

    make_jobs(snaps, dst_volume, src_subdir, *args, **kwargs, is_zfs=True)


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
    parser.add_argument('-a', '--all', action='store_true')
    parser.add_argument('-n', '--dry-run', action='count', default=0)
    parser.add_argument('-t', '--threads', type=int, default=4)
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-c', '--count', type=int, default=0)
    parser.add_argument('sets', nargs='*')
    args = parser.parse_args()

    # self.dry_run = True

    if args.all:
        args.sets = ['main', 'artifacts', 'cache']
    if not args.sets:
        print("Provide a set name.")
        exit(1)

    for set_ in args.sets:
        do_set(args, set_)

def do_set(args, set_):

    Index._cache.clear()

    if set_ == 'main':

        make_timestamped_jobs('work', 'tank/sitgmain',
            dst_subdir='work',
        )

        make_zfs_jobs('tank/heap/sitg', 'tank/sitgmain',
            ignore=set(('backups', 'cache', 'out', 'out-nosync', 'work')),
            skip_start=True,
        )

        make_zfs_jobs('tank/heap/sitg/work', 'tank/sitgmain',
            dst_subdir='work',
            ignore=set(('artifacts-film', 'cache-film')),
        )

    if set_ == 'artifacts':

        make_timestamped_jobs('out', 'tank/sitgartifacts',
            dst_subdir='trailer',
        )

        make_zfs_jobs('tank/heap/sitg/work', 'tank/sitgartifacts',
            src_subdir='artifacts-film',
            ignore=set(('trailer', )),
        )

    if set_ == 'cache':

        # Seed the caches. We only have the one set from the trailer.
        jobs.append(SyncJob(
            'tank/sitgcache',
            '2015-08-02T00.sitg',
            target='/mnt/tank/sitgcache/TE',
            a='/mnt/tank/sitgcache/TE',
            b='/mnt/tank/heap/sitg/cache',
        ))

        # The work caches.
        make_zfs_jobs('tank/heap/sitg/work', 'tank/sitgcache',
            src_subdir='cache-film',
            ignore=set(('TE', )),
        )

    if set_ == 'test':

        make_zfs_jobs('tank/test/src', 'tank/test/dst',
            ignore=['ignore'],
            skip_start=True,
        )


    jobs.sort(key=lambda j: j.sort_key)

    assert len(set(j.volname for j in jobs)) == 1


    snaps = get_zfs_snapshots(jobs[0].volname)

    cmd = ['zfs', 'rollback', snaps[-1].fullname]
    if args.verbose > 1:
        print(' '.join(cmd))
    if not args.dry_run:
        subprocess.check_call(cmd)

    processor = Processor(
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    done = 0

    for job in jobs:

        click.secho(f'==> {job.__class__.__name__} {job.volname}@{job.snapname}', fg='blue')
        click.echo(f'target: {job.target}')
        click.echo(f'src a:  {job.a}')
        click.echo(f'src b:  {job.b}')
        if job.ignore:
            click.echo(f'ignore: {" ".join(sorted(job.ignore))}')
        if job.is_zfs:
            click.echo(f'is_zfs: true')

        existing = next((s for s in snaps if s.name == job.snapname), None)
        if existing:
            click.secho(f'Already done at {existing.creation.isoformat("T")}', fg='green')
            continue

        if args.dry_run < 2:
            click.echo('---')
            try:
                job.run(processor, threads=args.threads)
            except Exception as e:
                click.secho(f'{e.__class__.__name__}: {e}', fg='red')
                pdb.post_mortem()
                raise # Don't let us keep going.

        cmd = ['zfs', 'snapshot', f'{job.volname}@{job.snapname}']
        if args.verbose > 1:
            print(' '.join(cmd))
        if not args.dry_run:
            subprocess.check_call(cmd)

        done += 1
        if args.count and not args.dry_run and args.count >= done:
            break



