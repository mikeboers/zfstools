import subprocess
import re


def get_block(dataset, object):

    if '@' not in dataset:
        raise ValueError("Must be given snapshot.", dataset)

    ret = None

    proc = subprocess.Popen(['zdb', '-ddddd', dataset, str(object)], stdout=subprocess.PIPE)
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

