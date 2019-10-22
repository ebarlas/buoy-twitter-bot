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

LOCAL_TZ = 'America/Los_Angeles'


def init_logging():
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] <%(threadName)s> %(levelname)s - %(message)s')


def first_sentence(latest, time_pacific):
    hour_minute = time_pacific.strftime('%-I:%M %p')
    wave_height = latest['wave_height'] * FEET_PER_METER
    return (f'At {hour_minute}, wave height was reported at {round(wave_height, 1)} ft '
            f'with a period of {int(latest["dominant_period"])} seconds. ')


def second_sentence(db, latest, time_pacific):
    month_per = db.query_month_percentile(latest['month'], latest['wave_height'])[0]
    month_day_per = db.query_month_day_percentile(latest['month_day'], latest['wave_height'])[0]
    month = time_pacific.strftime('%B')
    month_day = time_pacific.strftime('%B %-d')
    return (f'The wave height exceeds {month_per} percent of historical records for {month} '
            f'and {month_day_per} percent for {month_day}. ')


def third_sentence(db, latest):
    last_occurrence = db.find_last_occurrence_of(latest['wave_height'])
    time_utc = datetime.datetime(
        int(last_occurrence['year']['N']),
        int(last_occurrence['month']['N']),
        int(last_occurrence['day']['N']),
        int(last_occurrence['hour']['N']),
        int(last_occurrence['minute']['N']),
        tzinfo=pytz.utc)
    time_pacific = time_utc.astimezone(pytz.timezone(LOCAL_TZ))
    date = time_pacific.strftime('%-I:%M %p on %B %-d, %Y')
    wave_height = float(last_occurrence['waveheight']['N']) * FEET_PER_METER
    return f'The last observation at that height was {round(wave_height, 1)} ft at {date}.'


def write_paragraph(db, latest):
    time_utc = datetime.datetime(
        latest['year'],
        latest['month'],
        latest['day'],
        latest['hour'],
        latest['minute'],
        tzinfo=pytz.utc)
    time_pacific = time_utc.astimezone(pytz.timezone(LOCAL_TZ))
    return (f'{first_sentence(latest, time_pacific)}'
            f'{second_sentence(db, latest, time_pacific)}'
            f'{third_sentence(db, latest)}')


def main(table, buoy):
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

    p = write_paragraph(db, noaa_latest)
    logger.info(p)

    db.write_conditional(difference)


def lambda_handler(event, context):
    table = os.environ['table']
    buoy = os.environ['buoy']
    main(table, buoy)


if __name__ == '__main__':
    main('buoy-observations', '46013')
