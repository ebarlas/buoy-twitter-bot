import time
import logging

FIRST_BACKOFF = 0.1  # 100ms, 200ms, 400ms, ...
MAX_BACKOFF = 5

DYNAMO_CHUNK_SIZE = 25

logger = logging.getLogger(__name__)


def _backoff_generator():
    t = FIRST_BACKOFF
    while True:
        yield t
        t = min(MAX_BACKOFF, t * 2)


def _batch_write(dynamo, items):
    bg = _backoff_generator()
    while items:
        response = dynamo.batch_write_item(RequestItems=items)
        items = response['UnprocessedItems']
        if items:
            sleep = next(bg)
            logger.debug(f'unprocessed items, sleeping for {sleep} seconds')
            time.sleep(sleep)


def _partition(list):
    return [list[x:x + DYNAMO_CHUNK_SIZE] for x in range(0, len(list), DYNAMO_CHUNK_SIZE)]


def batch_put_items(dynamo, table_name, items):
    sum = 0
    partitions = _partition(items)
    for partition in partitions:
        batch = {table_name: [{'PutRequest': {'Item': item}} for item in partition]}
        _batch_write(dynamo, batch)
        sum += len(partition)
        logger.debug(f'wrote batch of {len(partition)}, total written is {sum}')
