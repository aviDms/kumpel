from functools import wraps
import datetime


def read_sql(path, **kwargs):
    with open(path, 'r') as handle:
        sql = handle.read()
        for arg, value in kwargs.items():
            to_be_replaced = '{' + arg + '}'
            if isinstance(value, list):
                replace_with = ', '.join(value)
            else:
                replace_with = str(value)
            sql = sql.replace(to_be_replaced, replace_with)
    print(sql)
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


def date_split(start_date, end_date, window=30):
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
    assert isinstance(start_date, datetime.date)
    assert isinstance(end_date, datetime.date)
    assert start_date < end_date

    if (end_date - start_date).days > window:
        seq = []
        date = start_date
        while date < end_date:
            seq.append(date)
            date += datetime.timedelta(days=window)
        seq.append(end_date)

        intervals = []
        one_day = datetime.timedelta(days=1)
        for i, date in enumerate(seq[:-2]):
            intervals.append((date, (seq[i+1] - one_day)))
        intervals.append((seq[-2], (seq[-1])))
        return intervals
    else:
        return [(start_date, end_date), ]
