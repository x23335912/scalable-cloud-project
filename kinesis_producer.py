
import boto3
import json
import time

kinesis = boto3.client('kinesis', region_name='us-east-1')

def send_data_to_kinesis(stream_name, data_file):
    with open(data_file) as f:
        for line in f:
            data = {'text': line.strip()}
            print(f"Sending: {data}")
            kinesis.put_record(
                StreamName=stream_name,
                Data=json.dumps(data),
                PartitionKey="partitionKey"
            )
            time.sleep(0.5)  # simulate real-time

send_data_to_kinesis("text-stream", "sample.txt")
