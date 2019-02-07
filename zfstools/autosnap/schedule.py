import datetime
import re


def parse_datetime(input_):

    for pattern in (
        r'(\d{4})-(\d{2})-(\d{2})',
        r'(\d{4})-(\d{2})-(\d{2})[T -](\d{2})',
        r'(\d{4})-(\d{2})-(\d{2})[T -](\d{2})[:-](\d{2})',
        r'(\d{4})-(\d{2})-(\d{2})[T -](\d{2})[:-](\d{2})[:-]?(\d{2})',
    ):
        m = re.match(r'^%s(?:-\d{2}:?\d{2})?$' % pattern, input_)
        if not m:
            continue
        
        ints = map(int, m.groups())
        while len(ints) < 6:
            ints.append(0)

        return datetime.datetime(*ints)


def label_snapshots(snapshots):
    """Label the given snapshots with what retention period they cover.

    Labels are:

    - ``latest`` for the latest;
    - ``all`` for last week;
    - ``daily`` for last 2 weeks;
    - ``weekly`` for last ~2 months;
    - ``montly`` forever;
    - ``first`` for the first.

    :param snapshots: List of ``(ctime, name)`` tuples representing a set of
        snapshots for a volume.
    :returns: Dict mapping ``name`` to a label (if the snapshot is to be kept).

    """

    # TODO: What timezone should this be in?
    now = datetime.datetime.now()

    to_keep = {}
    by_period = {}

    for i, (name, ctime) in enumerate(sorted(snapshots)):

        #if not i:
        #    by_period[(4, 'first', None)] = name
        
        if i + 1 == len(snapshots):
            by_period[(-1, 'latest', None)] = name

        if isinstance(ctime, basestring):
            ctime = parse_datetime(ctime)
            if not ctime:
                to_keep[name] = 'unknown'
                continue

        days = (now - ctime).days

        # Monthly forever.
        if True:
            month = ctime.replace(day=1, hour=0, minute=0, second=0)
            by_period.setdefault((3, 'monthly', month), name)

        # Weekly for ~2 months:
        if days < 2 * 31:
            week = (ctime - datetime.timedelta(days=ctime.weekday())).replace(hour=0, minute=0, second=0)
            by_period.setdefault((2, 'weekly', week), name)

        # Daily for 2 weeks.
        if days < 14:
            day = ctime.replace(hour=0, minute=0, second=0)
            by_period.setdefault((1, 'daily', day), name)

        # All for 1 week.
        if days < 7:
            by_period[(0, 'all', ctime)] = name
    
    to_keep.update({name: label for (_, label, _), name in sorted(by_period.iteritems(), reverse=True)})
    return to_keep


