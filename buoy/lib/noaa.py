import requests
from collections import namedtuple

Month = namedtuple('Month', 'id name')

MONTH_JAN = Month(1, 'Jan')
MONTH_FEB = Month(2, 'Feb')
MONTH_MAR = Month(3, 'Mar')
MONTH_APR = Month(4, 'Apr')
MONTH_MAY = Month(5, 'May')
MONTH_JUN = Month(6, 'Jun')
MONTH_JUL = Month(7, 'Jul')
MONTH_AUG = Month(8, 'Aug')
MONTH_SEP = Month(9, 'Sep')
MONTH_OCT = Month(10, 'Oct')
MONTH_NOV = Month(11, 'Nov')
MONTH_DEC = Month(12, 'Dec')

MONTHS = [MONTH_JAN, MONTH_FEB, MONTH_MAR, MONTH_APR, MONTH_MAY, MONTH_JUN,
          MONTH_JUL, MONTH_AUG, MONTH_SEP, MONTH_OCT, MONTH_NOV, MONTH_DEC]

URL_BASE = 'https://www.ndbc.noaa.gov'

URL_HISTORICAL = URL_BASE + '/view_text_file.php'
URL_YEAR = URL_HISTORICAL + '?filename={buoy}h{year}.txt.gz&dir=data/historical/stdmet/'
URL_YEAR_MONTH = URL_HISTORICAL + '?filename={buoy}{month_id}{year}.txt.gz&dir=data/stdmet/{month_name}/'

URL_REALTIME = URL_BASE + '/data'
URL_MONTH = URL_REALTIME + '/stdmet/{month_name}/{buoy}.txt'
URL_LAST_45 = URL_REALTIME + '/realtime2/{buoy}.txt'
URL_LAST_5 = URL_REALTIME + '/5day2/{buoy}_5day.txt'


def fetch_data(url, **kwargs):
    url = url.format(**kwargs)
    res = requests.get(url)
    res.raise_for_status()
    return res.text


def fetch_buoy_data_year(buoy, year):
    return fetch_data(URL_YEAR, buoy=buoy, year=year)


def fetch_buoy_data_year_month(buoy, year, month):
    return fetch_data(URL_YEAR_MONTH, buoy=buoy, year=year, month_id=month.id, month_name=month.name)


def fetch_buoy_data_month(buoy, month):
    return fetch_data(URL_MONTH, buoy=buoy, month_name=month.name)


def fetch_buoy_data_last45(buoy):
    return fetch_data(URL_LAST_45, buoy=buoy)


def fetch_buoy_data_last5(buoy):
    return fetch_data(URL_LAST_5, buoy=buoy)


def resolve_month(name):
    filtered = [month for month in MONTHS if month[1] == name]
    return filtered[0] if filtered else MONTH_JAN


def main():
    print(fetch_buoy_data_year('46013', '1981'))
    print(fetch_buoy_data_last45('46013'))
    print(fetch_buoy_data_last5('46013'))


if __name__ == '__main__':
    main()
