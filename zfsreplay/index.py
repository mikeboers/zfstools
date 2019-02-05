import collections
import os
import stat

from .utils import cached_property


BaseNode = collections.namedtuple('BaseNode', 'name path relpath fmt is_dir is_file is_link stat')

class Node(BaseNode):

    @cached_property
    def link_dest(self):
        return os.readlink(self.path)



def walk(root, ignore=None, rel_root=None, root_dev=None):

    if root_dev is None:
        root_dev = os.stat(root).st_dev
    if rel_root is None:
        rel_root = root
    
    for name in sorted(os.listdir(root)):

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
            yield from walk(path, rel_root=rel_root, root_dev=root_dev)


class Index(object):

    _cache = {}

    @classmethod
    def get(cls, root, ignore=None):

        ignore = set(ignore or ())
        key = (root, tuple(ignore))

        try:
            return cls._cache[key]
        except KeyError:
            pass

        print(f'==> Indexing {root} ignoring {ignore or None}')

        self = cls(root, ignore)
        self.go()
        cls._cache[key] = self
        
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
