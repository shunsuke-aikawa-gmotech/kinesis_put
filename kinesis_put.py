import sys
import time
import json
import boto3
from boto3.session import Session



client = boto3.client('kinesis')


def put_kinesis(json, key):
    try:
        res = client.put_record(
            # StreamName = 'tukada_new_log',
            StreamName = 'tebasaki_log',
            Data = json,
            PartitionKey = key
        )
        print res

    except Exception as e:
       print 'Kinesis put record excption'
       print e.message
       sys.exit





def main(key):

    try:

        #fake data
        data = {
            "aaa":1,
            "bbb":2
        }
        data['test'] = 'momonga'
        put_kinesis(json.dumps(data), key)



    except Exception as e:
        print 'Exception exit'
        print e.message
        sys.exit




if __name__ == '__main__':
    print '----------start-------------'
    for i in range(100):
        if i % 2 == 0:
            key = 'v1'
        else:
            key = 'v7'

        main(key)
