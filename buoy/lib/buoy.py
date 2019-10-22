import requests
import datetime
import pytz

MONTH_JAN = (1, 'Jan')
MONTH_FEB = (2, 'Feb')
MONTH_MAR = (3, 'Mar')
MONTH_APR = (4, 'Apr')
MONTH_MAY = (5, 'May')
MONTH_JUN = (6, 'Jun')
MONTH_JUL = (7, 'Jul')
MONTH_AUG = (8, 'Aug')
MONTH_SEP = (9, 'Sep')
MONTH_OCT = (10, 'Oct')
MONTH_NOV = (11, 'Nov')
MONTH_DEC = (12, 'Dec')

URL_BASE = 'https://www.ndbc.noaa.gov'
URL_HISTORICAL = URL_BASE + '/view_text_file.php'
URL_REALTIME = URL_BASE + '/data'

URL_YEAR = URL_HISTORICAL + '?filename={buoy_id}h{year}.txt.gz&dir=data/historical/stdmet/'
URL_YEAR_MONTH = URL_HISTORICAL + '?filename={buoy_id}{month_id}{year}.txt.gz&dir=data/stdmet/{month_name}/'
URL_MONTH = URL_REALTIME + '/stdmet/{month_name}/{buoy_id}.txt'
URL_LAST_45 = URL_REALTIME + '/realtime2/{buoy_id}.txt'
URL_LAST_5 = URL_REALTIME + '/5day2/{buoy_id}_5day.txt'


def fetch_buoy_data_year(buoy_id, year):
    url = URL_YEAR.format(buoy_id=buoy_id, year=year)
    return requests.get(url).text


def fetch_buoy_data_year_month(buoy_id, year, month):
    url = URL_YEAR_MONTH.format(buoy_id=buoy_id, year=year, month_id=month[0], month_name=month[1])
    return requests.get(url).text


def fetch_buoy_data_month(buoy_id, month):
    url = URL_MONTH.format(buoy_id=buoy_id, month_name=month[1])
    return requests.get(url).text


def fetch_buoy_data_last45(buoy_id):
    url = URL_LAST_45.format(buoy_id=buoy_id)
    return requests.get(url).text


def fetch_buoy_data_last5(buoy_id):
    url = URL_LAST_5.format(buoy_id=buoy_id)
    return requests.get(url).text


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
        year_normal = year_short + 1900 if year_short else (year_long1 if year_long1 else year_long2)
        month = parse_int(words, index_month, (1, 12))
        day = parse_int(words, index_day, (1, 31))
        hour = parse_int(words, index_hour, (0, 23))
        minute = parse_int(words, index_minute, (0, 59))
        minute_normal = minute if minute is not None else 0
        wave_height = parse_float(words, index_wave_height, (0, 98))
        wave_dir = parse_float(words, index_wave_dir, (0, 360))
        dom_period = parse_float(words, index_dom_period, (0, 98))
        avg_period = parse_float(words, index_avg_period, (0, 98))

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


def annotate(records):
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


def parse_and_annotate(data):
    return annotate(parse_data(data))


def main():
    records = parse_and_annotate(fetch_buoy_data_year('46013', '1981'))
    print(len(records))
    for rec in records:
        print(rec)


if __name__ == '__main__':
    main()
