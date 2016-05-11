from functools import wraps


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
