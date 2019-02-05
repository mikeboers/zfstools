import os


class Processor(object):

    def __init__(self, dry_run=False, verbose=0):
        self.dry_run = dry_run
        self.verbose = verbose
        
    def rename(self, src, dst):
        if self.verbose:
            print('rename ', src, dst)
        if not self.dry_run:
            os.rename(src, dst)

    def rmdir(self, path):
        if self.verbose:
            print('rmdir  ', path)
        if not self.dry_run:
            os.rmdir(path)

    def unlink(self, path, verbosity=1):
        if self.verbose >= verbosity:
            print('unlink ', path)
        if not self.dry_run:
            os.unlink(path)

    def mkdir(self, path):
        if self.verbose:
            print('mkdir ', path)
        if not self.dry_run:
            os.mkdir(path)
    def symlink(self, dst, src):
        if self.verbose:
            print('symlink', dst, src)
        if not self.try_run:
            os.symlink(dst, src)

    def chmod(self, path, mode, verbosity=1):
        if self.verbose >= verbosity:
            print('chmod  ', mode, path)
        if not self.dry_run:
            os.chmod(path, mode) #, follow_symlinks=False)

    def chown(self, path, uid, gid, verbosity=1):
        if self.verbose >= verbosity:
            print(f'chown   {uid}:{gid} {path}')
        if not self.dry_run:
            os.chown(path, uid, gid, follow_symlinks=False)

    def utime(self, path, atime, mtime, verbosity=1):
        if self.verbose >= verbosity:
            print(f'utime   {atime}:{mtime} {path}')
        if not self.dry_run:
            os.utime(path, (atime, mtime), follow_symlinks=False)

    def copy(self, src_path, dst_path):

        if self.verbose:
            print('copy   ', src_path, dst_path)
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

        if self.verbose:
            print('merge  ', src_path, dst_path)
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


