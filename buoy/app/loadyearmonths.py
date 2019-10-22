import argparse
import logging
import boto3
from buoy.lib import dynamo
from buoy.lib import noaa
from buoy.lib import parse
from buoy.lib import loginit

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--table', help="DynamoDB table name", required=True)
    parser.add_argument('-r', '--region', help="DynamoDB table region", required=True)
    parser.add_argument('-y', '--year', help="Four-digit year", type=int, required=True)
    parser.add_argument('-m', '--month', help="Three-letter month name", required=True)
    parser.add_argument('-c', '--count', help="Number of consecutive months", type=int, required=True)
    parser.add_argument('-b', '--buoy', help="NOAA buoy identifier", required=True)
    parser.add_argument('-p', '--prefix', help="Log file prefix", required=True)
    args = parser.parse_args()

    loginit.init_logger(args.prefix)
    logger.info(f'args: {args}')

    month = noaa.resolve_month(args.month)

    client = boto3.client('dynamodb', region_name=args.region)
    db = dynamo.Dynamo(client, args.table, args.buoy)

    for n in range(args.count):
        m = noaa.MONTHS[month[0] + n - 1]
        records = parse.parse_normalize_filter(noaa.fetch_buoy_data_year_month(args.buoy, args.year, m))
        db.write(records)


if __name__ == '__main__':
    main()
