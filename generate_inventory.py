'''Script will generate sample stream data and will push to kinesis
:number of records
:stream name'''
import sys
from datetime import datetime
from datetime import timedelta
import json
from collections import defaultdict
import boto3



def check_for_parm(rate,stream_name):
    '''this function checks for input params'''

    if not rate:
        return "number of streams is missing"
    if not stream_name:
        return "aws stream name is missing"

    return "all params are there"


def generate_records(number):
    '''needs to be rewritten
    this function generates different units
    for two products and timeseries'''
    sample_products = ['DHI','Bat']
    key = ['product', 'units', 'datetime']
    datetime_x = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    dict_x = defaultdict(dict)
    for i in range(number):
        for k, val in enumerate(key):
            if k == 0:
                if (i % 2) == 0:
                    dict_x[i][val] = sample_products[0]
                else:
                    dict_x[i][val] = sample_products[1]
            elif k == 1:
                dict_x[i][val] = i*100
            else:
                datetime_x = datetime.now() + timedelta(milliseconds=500)
                dict_x[i][val] = datetime_x.strftime('%Y-%m-%d %H:%M:%S.%f')

    data = dict_x

    return data


def main():
    '''this function will parse input params and will
         create data objects'''
    try:
        num = int(sys.argv[1])
        stream_name = sys.argv[2]
        check_for_parm(num,stream_name)
        data = generate_records(num)
        kinesis_client = boto3.client('kinesis')
        for value in data.items():
            value_x = json.dumps(value)
            input_x = {'Data': value_x, 'PartitionKey': '1'}
            response = kinesis_client.put_records(
                StreamName=stream_name,
                Records=[input_x],
                )
            print(response)


    except:
        print("Error:", sys.exc_info()[0])
        raise


if __name__ == "__main__":
    main()
