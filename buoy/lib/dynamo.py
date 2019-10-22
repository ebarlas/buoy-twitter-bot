import logging
from botocore.exceptions import ClientError
from buoy.lib import dbquery
from buoy.lib import batchput

logger = logging.getLogger(__name__)


class Dynamo:
    def __init__(self, client, table, buoy):
        self.client = client
        self.table = table
        self.buoy = buoy

    def write(self, records):
        """
        Write list of record dictionaries as items into DynamoDB table.
        Eagerly write inline index items into table as well.
        """
        self._write(records)
        self._write_index(records)

    def write_conditional(self, records):
        """
        Write list of record dictionaries as items into DynamoDB table.
        Conditionally write inline index items into table as well.
        """
        self._write(records)
        self._write_index_conditional(records)

    def _write(self, records):
        """
        Write list of record dictionaries as items into DynamoDB table.
        """
        items = self._convert_items(records)
        logger.info(f'converted {len(items)} items, batch-writing to dynamodb')
        batchput.batch_put_items(self.client, self.table, items)

    def _write_index(self, records):
        """
        Eagerly write list of record dictionaries as inline index items into DynamoDB table.
        """
        items = self._convert_index_items(records)
        logger.info(f'writing index of size {len(items)}')
        batchput.batch_put_items(self.client, self.table, items)
        for item in items:
            logger.debug(f'wrote index item: {item}')

    def _write_index_conditional(self, records):
        """
        Conditionally write list of record dictionaries as inline index items into DynamoDB table.
        """
        items = self._convert_index_items(records)
        logger.info(f'conditionally writing index of size {len(items)}')
        self._write_index_items_conditional(items)

    def _convert_items(self, records):
        """
        Convert list of record dictionaries to list of DyanmoDB item dictionaries.
        """
        items = []
        for n, record in enumerate(records):
            logger.debug(f'converting record {n + 1}: {record}')
            item = self._convert_item(record)
            logger.debug(f'converted record {n + 1} to item: {item}')
            items.append(item)
        return items

    def _convert_item(self, record):
        """
        Make Dynamo DB table item dictionary from record dictionary. See parse.py for record specification.
        """
        item = {
            'id': {'S': self.buoy},
            'time': {'S': record['time']},
            'year': {'N': str(record['year'])},
            'month': {'N': str(record['month'])},
            'day': {'N': str(record['day'])},
            'hour': {'N': str(record['hour'])},
            'minute': {'N': str(record['minute'])},
            'monthday': {'S': record['month_day']},
            'yearmonth': {'S': record['year_month']},
            'waveheight': {'N': str(record['wave_height'])}
        }
        if record['wave_direction']:
            item['wavedir'] = {'N': str(record['wave_direction'])}
        if record['dominant_period']:
            item['domperiod'] = {'N': str(record['dominant_period'])}
        if record['average_period']:
            item['avgperiod'] = {'N': str(record['average_period'])}
        return item

    def _convert_index_items(self, records):
        """
        Converts list of record dictionaries to list of inline index items.
        """
        index = {}
        for record in records:
            ym = record['year_month']
            if ym not in index or record['wave_height'] > index[ym]['wave_height']:
                index[ym] = record
        return [self._convert_index_item(record) for record in index.values()]

    def _convert_index_item(self, record):
        """
        Make DynamoDB table item for inline year-month wave-height index.
        """
        item = self._convert_item(record)
        item['id'] = {'S': f'{self.buoy}/yearmonth'}
        item['time'] = {'S': record['year_month']}
        return item

    def _write_index_items_conditional(self, items):
        """
        Conditionally write inline index items.
        """
        for item in items:
            self._write_index_item_conditional(item)

    def _write_index_item_conditional(self, item):
        """
        Conditionally write inline index item if item not already present or if prior item has smaller wave height.
        """
        try:
            self.client.put_item(
                TableName=self.table,
                Item=item,
                ConditionExpression='attribute_not_exists(#time) OR #waveheight < :waveheight',
                ExpressionAttributeNames={
                    '#waveheight': 'waveheight',
                    '#time': 'time'
                },
                ExpressionAttributeValues={
                    ':waveheight': item['waveheight']
                }
            )
            logger.debug(f'wrote index item: {item}')
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                logger.debug(f'did not write index item due to condition check failure: {item}')
            else:
                raise e

    def find_max_wave_height(self):
        """
        Find the maximum wave height ever recorded by scanning through inline index records.
        """
        max_item = None
        for item in dbquery.item_generator(self._query_index_page):
            if not max_item or float(item['waveheight']['N']) > float(max_item['waveheight']['N']):
                max_item = item
        return max_item

    def find_last_occurrence_of(self, wave_height):
        """
        Find the most recent occurrence of a wave height greater than or equal to the input wave height.
        """
        index_item = dbquery.first_item(lambda k: self._query_index_page(wave_height, k))
        if not index_item:
            return

        year_month = index_item['time']['S']
        item = dbquery.first_item(lambda k: self._query_items_page(wave_height, year_month, k))
        if item:
            return item

        logger.warning(f'located index item but failed to locate individual record for year-month {year_month}')

    def find_latest(self):
        """
        Find the most recent item in the database.
        """
        res = self._query_latest()
        if res['Items']:
            return res['Items'][0]

    def _query_latest(self):
        """
        Query most recent item using range key scan backward.
        """
        return self.client.query(
            TableName=self.table,
            ScanIndexForward=False,
            Limit=1,
            KeyConditionExpression='#id = :id',
            ExpressionAttributeNames={
                '#id': 'id'
            },
            ExpressionAttributeValues={
                ':id': {
                    'S': f'{self.buoy}'
                }
            }
        )

    def _query_index_page(self, wave_height=None, start_key=None):
        """
        Query page of wave height records using inline index.
        """
        params = {
            'TableName': self.table,
            'ScanIndexForward': False,
            'KeyConditionExpression': '#id = :id',
            'ExpressionAttributeNames': {
                '#id': 'id'
            },
            'ExpressionAttributeValues': {
                ':id': {
                    'S': f'{self.buoy}/yearmonth'
                }
            }
        }

        if wave_height:
            params['FilterExpression'] = 'waveheight >= :waveheight'
            params['ExpressionAttributeValues'][':waveheight'] = {
                'N': str(wave_height)
            }

        if start_key:
            params['ExclusiveStartKey'] = start_key

        return self.client.query(**params)

    def _query_items_page(self, wave_height, year_month, start_key=None):
        """
        Query page of items with input year-month and with wave height greater than input wave height.
        """
        params = {
            'TableName': self.table,
            'ScanIndexForward': False,
            'FilterExpression': 'waveheight >= :waveheight',
            'KeyConditionExpression': '#id = :id AND begins_with(#time, :yearmonth)',
            'ExpressionAttributeNames': {
                '#id': 'id',
                '#time': 'time'
            },
            'ExpressionAttributeValues': {
                ':id': {
                    'S': f'{self.buoy}'
                },
                ':yearmonth': {
                    'S': year_month
                },
                ':waveheight': {
                    'N': str(wave_height)
                }
            }
        }

        if start_key:
            params['ExclusiveStartKey'] = start_key

        return self.client.query(**params)

    def query_month_day(self, month_day):
        """
        Obtain all wave height values for a given month-day and return in sorted list ascending.
        """
        return dbquery.collect_and_sort(lambda k: self._query_month_day_page(month_day, k), 'waveheight')

    def query_month_day_percentile(self, month_day, wave_height):
        """
        Obtain all wave height values for a given month-day and calculate percentile of input value.
        """
        return dbquery.percentile(lambda k: self._query_month_day_page(month_day, k), 'waveheight', wave_height)

    def query_month(self, month):
        """
        Obtain all wave height values for a given month and return in sorted list ascending.
        """
        return dbquery.collect_and_sort(lambda k: self._query_month_page(month, k), 'waveheight')

    def query_month_percentile(self, month, wave_height):
        """
        Obtain all wave height values for a given month and calculate percentile of input value.
        """
        return dbquery.percentile(lambda k: self._query_month_page(month, k), 'waveheight', wave_height)

    def _query_month_day_page(self, month_day, start_key=None):
        """
        Query page of items from month-day index.
        """
        params = {
            'TableName': self.table,
            'IndexName': 'id-monthday',
            'ProjectionExpression': 'waveheight',
            'KeyConditionExpression': '#id = :id AND #monthday = :monthday',
            'ExpressionAttributeNames': {
                '#id': 'id',
                '#monthday': 'monthday'
            },
            'ReturnConsumedCapacity': 'TOTAL',
            'ExpressionAttributeValues': {
                ':id': {
                    'S': f'{self.buoy}'
                },
                ':monthday': {
                    'S': month_day
                }
            }
        }

        if start_key:
            params['ExclusiveStartKey'] = start_key

        return self.client.query(**params)

    def _query_month_page(self, month, start_key=None):
        """
        Query page of items from month index.
        """
        params = {
            'TableName': self.table,
            'IndexName': 'id-month',
            'ProjectionExpression': 'waveheight',
            'KeyConditionExpression': '#id = :id AND #month = :month',
            'ExpressionAttributeNames': {
                '#id': 'id',
                '#month': 'month'
            },
            'ReturnConsumedCapacity': 'TOTAL',
            'ExpressionAttributeValues': {
                ':id': {
                    'S': f'{self.buoy}'
                },
                ':month': {
                    'N': str(month)
                }
            }
        }

        if start_key:
            params['ExclusiveStartKey'] = start_key

        return self.client.query(**params)
