import os
import logging
import boto3
import datetime
import pytz
from buoy.lib import dynamo
from buoy.lib import noaa
from buoy.lib import parse

logger = logging.getLogger(__name__)

FEET_PER_METER = 3.28084

LOCAL_TZ = pytz.timezone('America/Los_Angeles')

TMP_FILE = '/tmp/waves.png'


def init_logging():
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] <%(threadName)s> %(levelname)s - %(message)s')


def deg_to_compass(num):
    """
    https://stackoverflow.com/questions/7490660/converting-wind-direction-in-angles-to-text-words
    """
    val = int((num / 22.5) + .5)
    arr = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return arr[(val % 16)]


def first_sentence(latest, pacific_time):
    hour_minute = pacific_time.strftime('%-I:%M %p')
    wave_height = latest['wave_height'] * FEET_PER_METER
    wave_dir = int(latest["wave_direction"])
    return (f'At {hour_minute}, significant waves measured {round(wave_height, 1)} ft '
            f'at {int(latest["dominant_period"])} second intervals from {wave_dir} '
            f'deg {deg_to_compass(wave_dir)}. ')


def second_sentence(db, latest, pacific_time):
    month_per = db.query_month_percentile(latest['month'], latest['wave_height'])[0]
    month_day_per = db.query_month_day_percentile(latest['month_day'], latest['wave_height'])[0]
    month = pacific_time.strftime('%B')
    month_day = pacific_time.strftime('%B %-d')
    return (f'The wave height exceeds {month_per} percent of historical observations for {month} '
            f'and {month_day_per} percent for {month_day}. ')


def third_sentence(db, latest):
    item = db.find_last_occurrence_of(latest['wave_height'])
    pacific_time = table_item_pacific_time(item)
    date = pacific_time.strftime('%-I:%M %p on %B %-d, %Y')
    wave_height = float(item['waveheight']['N']) * FEET_PER_METER
    return f'The last observation at that height was {round(wave_height, 1)} ft at {date}.'


def write_paragraph(db, latest):
    pacific_time = noaa_record_pacific_time(latest)
    return (f'{first_sentence(latest, pacific_time)}'
            f'{second_sentence(db, latest, pacific_time)}'
            f'{third_sentence(db, latest)}')


def table_item_pacific_time(item):
    return datetime.datetime(
        int(item['year']['N']),
        int(item['month']['N']),
        int(item['day']['N']),
        int(item['hour']['N']),
        int(item['minute']['N']),
        tzinfo=pytz.utc).astimezone(LOCAL_TZ)


def noaa_record_pacific_time(record):
    return datetime.datetime(
        record['year'],
        record['month'],
        record['day'],
        record['hour'],
        record['minute'],
        tzinfo=pytz.utc).astimezone(LOCAL_TZ)


def make_plot(records):
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.dates import DateFormatter
    x = [noaa_record_pacific_time(rec) for rec in records]
    y = [round(rec['wave_height'] * FEET_PER_METER, 1) for rec in records]
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(x, y, '-bo')
    ax.grid(True)
    ax.set(xlabel="Date", ylabel="Feet", title="Significant Wave Height - Last 5 Days")
    ax.xaxis.set_minor_locator(mdates.HourLocator(interval=1, tz=LOCAL_TZ))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1, tz=LOCAL_TZ))
    ax.xaxis.set_major_formatter(DateFormatter("%m/%d", tz=LOCAL_TZ))
    plt.savefig(TMP_FILE)
    return TMP_FILE


def tweet(message, records, twitter_credentials):
    import twitter
    file_name = make_plot(records)
    with open(file_name, 'rb') as f:
        api = twitter.Api(**twitter_credentials)
        status = api.PostUpdate(message, media=f)
        logger.info(f'posted twitter update with id {status.id} and create time {status.created_at}')


def main(table, buoy, twitter_credentials=None):
    init_logging()
    client = boto3.client('dynamodb')
    db = dynamo.Dynamo(client, table, buoy)

    db_latest = db.find_latest()
    logger.info(f'queried latest from dynamodb, time is {db_latest["time"]["S"]}')

    noaa_records = parse.parse_normalize_filter(noaa.fetch_buoy_data_last5(buoy))
    noaa_latest = max(noaa_records, key=lambda r: r['time'])
    logger.info(f'fetched 5 days of buoy observations, latest record time is {noaa_latest["time"]}')

    difference = [record for record in noaa_records if record['time'] > db_latest['time']['S']]

    if not difference:
        logger.info(f'no new buoy observations, exiting')
        return

    paragraph = write_paragraph(db, noaa_latest)
    logger.info(paragraph)
    logger.info(f'twitter update length is {len(paragraph)} characters')

    db.write_conditional(difference)

    if twitter_credentials:
        tweet(paragraph, noaa_records, twitter_credentials)


def lambda_handler(event, context):
    table = os.environ['table']
    buoy = os.environ['buoy']
    twitter_credentials = {
        'consumer_key': os.environ['twitter_consumer_key'],
        'consumer_secret': os.environ['twitter_secret_key'],
        'access_token_key': os.environ['twitter_access_token_key'],
        'access_token_secret': os.environ['twitter_access_token_secret']
    }
    main(table, buoy, twitter_credentials)


if __name__ == '__main__':
    main('buoy-observations', '46013')
