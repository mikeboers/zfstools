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



_gen_proc = None


class ZDBError(EnvironmentError):
    pass

def get_gen(dataset, obj):

    global _gen_proc

    if _gen_proc is None:

        _gen_proc = subprocess.Popen([os.path.abspath(os.path.join(__file__, '..', 'zgen'))],
            bufsize=1,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            #universal_newlines=True,
        )

    # print('<<<', dataset, obj)
    _gen_proc.stdin.write(f'{dataset} {obj:d}\n'.encode())
    _gen_proc.stdin.flush()
    raw = _gen_proc.stdout.readline().decode().rstrip()
    # print(">>>", repr(raw))

    res = raw.split()

    if res[2] == 'ERROR':
        errno = int(res[3])
        error = ' '.join(res[4:])
        if errno == 2:
            return
        raise ZDBError(errno, error)

    if len(res) != 3:
        raise ValueError(f"zgen output malformed: {raw}")

    dsn = res[0]
    ino = int(res[1])
    gen = int(res[2])

    if dsn != dataset or ino != obj:
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

