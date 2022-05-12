import json

import boto3 as boto3
import datetime


class KinesisLoader:
    def upload_to_stream(self, data, stream_name):
        kinesis = boto3.client("kinesis")

        key = datetime.datetime.now().minute // 10 * 10
        for line in data:
            kinesis.put_record(
                Data=json.dumps(line, ensure_ascii=False),
                PartitionKey=str(key),
                StreamName=stream_name
        )
