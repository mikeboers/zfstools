import subprocess
import os
import re


def get_block(dataset, obj):

    if '@' not in dataset:
        raise ValueError("Must be given snapshot.", dataset)

    ret = None

    proc = subprocess.Popen(['zdb', '-ddddd', dataset, str(obj)], stdout=subprocess.PIPE)
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


_gen_procs = {}

def get_gen(dataset, obj):

    try:
        proc = _gen_procs[dataset]

    except KeyError:

        # This isn't the most efficient thing, but... eh.
        if len(_gen_procs) == 2:
            _gen_procs.clear()

        proc = subprocess.Popen([os.path.abspath(os.path.join(__file__, '..', 'zgen')), dataset],
            bufsize=0,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        _gen_procs[dataset] = proc

    proc.stdin.write(f'{obj:d}\n'.encode())

    raw = proc.stdout.readline().decode().rstrip()
    res = raw.split()

    if len(res) != 2:
        raise ValueError(f"zgen error: {raw}")

    ino = int(res[0])
    gen = int(res[1])

    if ino != obj:
        raise ValueError(f"zgen desync for {obj}: {raw}")

    return gen



if __name__ == '__main__':

    import time

    from .index import Index

    idx = Index.get('/mnt/tank/heap/sitg/work/.zfs/snapshot/2018-08-01T02:00:01-04:00')
    print('starting...')
    start = time.time()

    for n in idx.nodes:
        gen = get_gen('tank/heap/sitg/work@2018-08-01T02:00:01-04:00', n.stat.st_ino)
        # print(n.stat.st_ino, gen)

    dur = time.time() - start
    print(len(idx.nodes), dur)

