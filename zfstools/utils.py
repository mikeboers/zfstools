
class cached_property(object):

    def __init__(self, fget):
        self.fget = fget
        self.name = fget.__name__

    def __get__(self, obj, cls):
        if obj is None:
            return
        value = self.fget(obj)
        setattr(obj, self.name, value)
        return value


def format_bytes(x):

    suffix = ('', 'k', 'M', 'G', 'T', 'P')
    order = 0

    while order < 5 and x > 1000:
        x /= 1024
        order += 1

    return f'{x:.2f}{suffix[order]}B'

