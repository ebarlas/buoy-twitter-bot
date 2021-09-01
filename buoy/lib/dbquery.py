def item_generator(fn_query):
    """
    Generator that sends paginated queries using supplied function yielding results along the way.
    """
    res = None
    start_key = None
    while not res or start_key:
        res = fn_query(start_key)
        for item in res['Items']:
            yield item
        start_key = res.get('LastEvaluatedKey')


def first_item(fn_query):
    """
    Send paginated queries using supplied function until a result is obtained. Return first observed result.
    """
    try:
        return next(item_generator(fn_query))
    except StopIteration:
        return


def collect_values(fn_query, column):
    """
    Extract float column values from queried items and place into a list.
    """
    return [float(item[column]['N']) for item in item_generator(fn_query)]


def collect_and_sort(fn_query, column):
    """
    Extract float column values from queried items, place into a list, and sort.
    """
    values = collect_values(fn_query, column)
    values.sort()
    return values


def percentile(fn_query, column, value):
    """
    Extract and sort float column values from queried items and calculate percentile of input value.
    """
    values = collect_values(fn_query, column)
    cnt = sum([1 for v in values if v <= value])
    per = int(cnt / len(values) * 100)
    return per, cnt, len(values)
