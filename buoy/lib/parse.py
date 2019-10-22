import datetime
import pytz
import logging

logger = logging.getLogger(__name__)


def column_index_of(headers, target):
    for n in range(len(headers)):
        if headers[n] == target:
            return n


def parse_value(values, index, fn, range):
    try:
        n = fn(values[index])
        return n if range[0] <= n <= range[1] else None
    except:
        return None


def parse_int(values, index, range):
    return parse_value(values, index, int, range)


def parse_float(values, index, range):
    return parse_value(values, index, float, range)


def parse_data(data):
    lines = iter(data.splitlines())

    header_line = next(lines)
    if header_line.startswith('#'):
        next(lines)

    headers = header_line.split()

    index_year_short = column_index_of(headers, 'YY')
    index_year_long1 = column_index_of(headers, 'YYYY')
    index_year_long2 = column_index_of(headers, '#YY')
    index_month = column_index_of(headers, 'MM')
    index_day = column_index_of(headers, 'DD')
    index_hour = column_index_of(headers, 'hh')
    index_minute = column_index_of(headers, 'mm')
    index_wave_height = column_index_of(headers, 'WVHT')
    index_wave_dir = column_index_of(headers, 'MWD')
    index_dom_period = column_index_of(headers, 'DPD')
    index_avg_period = column_index_of(headers, 'APD')

    records = []

    for line in lines:
        words = line.split()
        year_short = parse_int(words, index_year_short, (70, 98))
        year_long1 = parse_int(words, index_year_long1, (1970, 2070))
        year_long2 = parse_int(words, index_year_long2, (1970, 2070))
        month = parse_int(words, index_month, (1, 12))
        day = parse_int(words, index_day, (1, 31))
        hour = parse_int(words, index_hour, (0, 23))
        minute = parse_int(words, index_minute, (0, 59))
        wave_height = parse_float(words, index_wave_height, (0, 98))
        wave_dir = parse_float(words, index_wave_dir, (0, 360))
        dom_period = parse_float(words, index_dom_period, (0, 98))
        avg_period = parse_float(words, index_avg_period, (0, 98))

        year_normal = year_short + 1900 if year_short else (year_long1 if year_long1 else year_long2)
        minute_normal = minute if minute is not None else 0

        record = {
            'year': year_normal,
            'month': month,
            'day': day,
            'hour': hour,
            'minute': minute_normal,
            'wave_height': wave_height,
            'wave_direction': wave_dir,
            'dominant_period': dom_period,
            'average_period': avg_period
        }

        records.append(record)

    return records


def has_bad_date(record):
    return (record['year'] is None
            or record['month'] is None
            or record['day'] is None
            or record['hour'] is None
            or record['minute'] is None)


def has_more_info_than(left, right):
    return ((left['dominant_period'] is not None and right['dominant_period'] is None)
            or (left['average_period'] is not None and right['average_period'] is None)
            or (left['wave_direction'] is not None and right['wave_direction'] is None))


def normalize(records):
    index = {}
    for record in records:
        if has_bad_date(record):
            record['reason'] = 'bad_date'
            continue

        date = datetime.datetime(
            record['year'],
            record['month'],
            record['day'],
            record['hour'],
            record['minute'],
            tzinfo=pytz.utc)

        date_time = date.strftime('%Y%m%d%H')
        year_month = date.strftime('%Y%m')
        month_day = date.strftime('%m%d')

        record['time'] = date_time
        record['year_month'] = year_month
        record['month_day'] = month_day

        if record['wave_height'] is None:
            record['reason'] = 'no_wave_height'
            continue

        prior = index.get(date_time)
        if prior:
            if has_more_info_than(record, prior):
                prior['reason'] = 'dup_time'
            else:
                record['reason'] = 'dup_time'
                continue

        index[date_time] = record

    return records


def filter_and_log(records):
    filtered = []
    for n, record in enumerate(records):
        logger.debug(f'record {n + 1}: {record}')
        if 'reason' not in record:
            filtered.append(record)
    logging.info(f'{len(filtered)} of {len(records)} records retained')
    return filtered


def parse_normalize_filter(data):
    return filter_and_log(normalize(parse_data(data)))