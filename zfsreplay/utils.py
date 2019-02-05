
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

