from functools import wraps
import datetime

def read_text_file():
        raise NotImplementedError


def read_sql(path, **kwargs):
    with open(path, 'r') as handle:
        sql = handle.read()
        for arg in kwargs:
            to_be_replaced = '{' + arg + '}'
            replace_with = str(kwargs[arg])
            sql = sql.replace(to_be_replaced, replace_with)
    return sql


def coroutine(function):
    """ Decorator used to initialize a coroutine.
    :param function: python function
    """
    @wraps(function)
    def start(*args, **kwargs):
        res = function(*args, **kwargs)
        next(res)
        return res
    return start


def month_split(start_date, end_date, window=30):
    """
    Calculate window size days split between two dates.

    ex. start_date = "2012-01-01"
        end_date = "2012-02-28"
        window = 31
    result => [
        (datetime.date(2012-01-01), datetime.date(2012-01-31)),
        (datetime.date(2012-02-01), datetime.date(2012-02-28))
    ]

    :param start_date: <datetime.date> or <string> as "YYYY-MM-DD"
    :param end_date: <datetime.date> or <string> as "YYYY-MM-DD"
    :param window: <integer> how many days an interval should have
    :return: list of datetime tuples
    """
    if not isinstance(start_date, datetime.date):
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    if not isinstance(end_date, datetime.date):
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    if (end_date - start_date).days > window:
        seq = []
        date = start_date
        while date < end_date:
            seq.append(date)
            date += datetime.timedelta(days=window)
        seq.append(end_date)
        print(seq)

        intervals = []
        one_day = datetime.timedelta(days=1)
        for i, date in enumerate(seq[:-2]):
            intervals.append((date, (seq[i+1] - one_day)))
        intervals.append((seq[-2], (seq[-1])))
        return intervals
    else:
        return [(start_date, end_date), ]
