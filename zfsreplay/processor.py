import os
import time
import stat

from .utils import format_bytes


def field(x):
    return f'{x:10s}'


class Processor(object):

    def __init__(self, dry_run=False, verbose=0):
        self.dry_run = dry_run
        self.verbose = verbose
        
    def rename(self, src, dst):
        if self.verbose:
            print(field('rename'), src, dst)
        if not self.dry_run:
            os.rename(src, dst)

    def rmdir(self, path):
        if self.verbose:
            print(field('rmdir'), path)
        if not self.dry_run:
            os.rmdir(path)

    def unlink(self, path, verbosity=1):
        if self.verbose >= verbosity:
            print(field('unlink'), path)
        if not self.dry_run:
            os.unlink(path)

    def mkdir(self, path):
        if self.verbose:
            print(field('mkdir'), path)
        if not self.dry_run:
            os.mkdir(path)
    def symlink(self, dst, src):
        if self.verbose:
            print(field('symlink'), dst, src)
        if not self.dry_run:
            os.symlink(dst, src)

    def chmod(self, path, mode, verbosity=1):
        mode = stat.S_IMODE(mode)
        if self.verbose >= verbosity:
            print(field('chmod'), stat.filemode(mode), path)
        if not self.dry_run:
            os.chmod(path, mode) #, follow_symlinks=False)

    def chown(self, path, uid, gid, verbosity=1):
        if self.verbose >= verbosity:
            print(field('chown'), f'{uid}:{gid} {path}')
        if not self.dry_run:
            os.chown(path, uid, gid, follow_symlinks=False)

    def utime(self, path, atime, mtime, verbosity=1):
        if self.verbose >= verbosity:
            print(field('utime'), f'{atime}:{mtime} {path}')
        if not self.dry_run:
            os.utime(path, (atime, mtime), follow_symlinks=False)

    def copy(self, src_path, dst_path):

        if self.verbose:
            print(field('copy'), src_path, dst_path)
        if self.dry_run:
            return

        size = 128 * 1024 # ZFS block size.
        copied = 0
        start = time.monotonic()

        with open(src_path, 'rb') as src, open(dst_path, 'wb') as dst:
            while True:
                chunk = src.read(size)
                if not chunk:
                    break
                dst.write(chunk)
                copied += len(chunk)

        if self.verbose > 1:
            duration = time.monotonic() - start
            rate = copied / duration
            print(field('copied'), f'{format_bytes(copied):>8s} in {duration:>6.2f}s at {format_bytes(rate):>8s}/s')

    def merge(self, src_path, dst_path):

        if self.verbose:
            print(field('merge'), src_path, dst_path)
        if self.dry_run:
            return

        size = 128 * 1024 # ZFS block size.

        n_diff = 0

        start = time.monotonic()
        read = written = 0

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

                read += len(a)

                # The blocks match; don't do anything.
                if a == b:
                    continue

                dst.seek(pos)
                dst.write(a)

                written += len(a)
                n_diff += 1

            # We gave up comparing; just finish it up normally.
            while a:
                a = src.read(size)
                if a:
                    dst.write(a)
                    read += len(a)
                    written += len(a)

        if self.verbose > 1:
            duration = time.monotonic() - start
            rate = read / duration
            print(field('merged'), f'{format_bytes(written):>8s} of {format_bytes(read):>8s} in {duration:>6.2f}s at {format_bytes(rate):>8s}/s:', dst_path)

        return n_diff




