import boto3
import json
import time

from boto3.dynamodb.conditions import Attr
from decimal import Decimal

def lambda_handler(event, context):
    # Dynamodb
    tablename = 'stockalert'
    table = boto3.resource('dynamodb').Table(tablename)

    # SNS Topic
    topic_arn = 'arn:aws:sns:us-east-1:279123003175:stock15'
    sns_client = boto3.client('sns')

    # Kinesis
    my_stream_name = 'kinesis_stocks'
    kinesis_client = boto3.client('kinesis', region_name='us-east-1')
    response = kinesis_client.describe_stream(StreamName=my_stream_name)
    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                       ShardId=my_shard_id,
                                                       ShardIteratorType='LATEST')
    my_shard_iterator = shard_iterator['ShardIterator']
    record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator)
    
    while 'NextShardIterator' in record_response:
        record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'])
        
        if len(record_response['Records']) > 0:
            for item in record_response['Records']:
                try:
                    readings = json.loads(item["Data"], parse_float=Decimal)
                    stock = readings.get('stock', '')  # Use 'get' method to handle missing 'stock' key
                    timestamp = readings['timestamp']
                    value = float(readings['current_value'])
                    high_52week = float(readings['52week_high'])
                    low_52week = float(readings['52week_low'])
                    stockdate = readings['stockdate']

                    if value >= 0.8 * high_52week or value <= 1.2 * low_52week:
                        db_response = table.scan(
                            FilterExpression=Attr('stockid').eq(stock) & Attr('stockdate').eq(stockdate)
                        )
                        if db_response['Count'] == 0:
                            sns_response = sns_client.publish(
                                TopicArn=topic_arn,
                                Message=json.dumps({'default': json.dumps(readings)}),
                                MessageStructure='json',
                                Subject="Price PoI reached for %s on %s" % (stock, stockdate)
                            )
                            table.put_item(Item={'stockid': stock, 'timestamp': timestamp, 'pricePoI': str(value),
                                                 '52week_high': str(high_52week), '52week_low': str(low_52week),
                                                 'stockdate': stockdate})
                except Exception as e:
                    print(f"Error processing record: {str(e)}")

        # wait for milli-seconds
        time.sleep(0.25)
