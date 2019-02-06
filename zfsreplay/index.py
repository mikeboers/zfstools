import collections
import os
import stat
import time

from .utils import cached_property


BaseNode = collections.namedtuple('BaseNode', 'name path relpath fmt is_dir is_file is_link stat')

class Node(BaseNode):

    @cached_property
    def link_dest(self):
        return os.readlink(self.path)



def walk(root, ignore=None, rel_root=None, root_dev=None, _depth=0):

    if rel_root is None:
        rel_root = root
    
    tries = 1 if _depth else 4
    for i in range(tries):
        if i:
            time.sleep(2 ** (i - 1)) # 1, 2, 4
        names = os.listdir(root)
        if names:
            break

    # We wait until after resolving names so that our stat isn't
    # empty or some other bullshit due to ZFS not giving us data
    # until we list the directory.
    if root_dev is None:
        root_dev = os.stat(root).st_dev

    for name in sorted(names):

        if ignore and name in ignore:
            continue

        path = os.path.join(root, name)
        st = os.lstat(path)

        if st.st_dev != root_dev:
            continue

        fmt = stat.S_IFMT(st.st_mode)
        is_dir = fmt == stat.S_IFDIR
        is_file = fmt == stat.S_IFREG
        is_link = fmt == stat.S_IFLNK
        if not (is_dir or is_file or is_link):
            continue

        yield Node(name, path, os.path.relpath(path, rel_root), fmt, is_dir, is_file, is_link, st)

        if is_dir:
            yield from walk(path, rel_root=rel_root, root_dev=root_dev, _depth=_depth+1)


class Index(object):

    _cache = {}

    @classmethod
    def get(cls, root, ignore=None, cache_key=None):

        ignore = set(ignore or ())
        cache_key = cache_key or (root, tuple(ignore))

        try:
            pass #return cls._cache[cache_key]
        except KeyError:
            pass

        print(f'Indexing {root} ignoring {ignore or None}')

        self = cls(root, ignore)
        self.go()
        #cls._cache[cache_key] = self
        
        print(f'    {len(self.by_ino)} inodes in {len(self.by_rel)} paths')

        return self

    def __init__(self, root, ignore):
        self.root = root
        self.ignore = ignore
        self.nodes = []
        self.by_ino = {}
        self.by_rel = {}

    def go(self):
        for node in walk(self.root, ignore=self.ignore):
            self.nodes.append(node)
            self.by_ino.setdefault(node.stat.st_ino, []).append(node)
            self.by_rel[node.relpath] = node


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--ignore', action='append')
    parser.add_argument('roots', nargs='+')
    args = parser.parse_args()

    for root in args.roots:

        print(root)
        idx = Index.get(root, ignore=args.ignore)

        n_ino = len(idx.by_ino)
        n_rel = len(idx.by_rel)
        print(f'{n_ino - n_rel} links; {n_ino} inodes in {n_rel} paths')
