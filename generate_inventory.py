import sys
from datetime import datetime
import json
from collections import defaultdict
import boto3



def check_for_parm(rate,stream_name):
    if not rate:
        return "number of streams is missing"
    elif not stream_name:
        return "aws stream name is missing"
    else:
        pass


def generate_records(number):
    '''needs to be rewritten'''
    sample_products = ['Auto','Bat']
    key = ['product', 'units', 'datetime']
    datetime_x = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    dict_x = defaultdict(dict)
    for i in range(number):
        for t, x in enumerate(key):
            if t == 0:
                if (i % 2) == 0:
                    dict_x[i][x] = sample_products[0]
                else:
                    dict_x[i][x] = sample_products[1]
            elif t == 1:
                dict_x[i][x] = i*100
            else:
                dict_x[i][x] = datetime_x

    data = dict_x

    return data


def main():

    try:
        num = int(sys.argv[1])
        stream_name = sys.argv[2]
        check_for_parm(num,stream_name)
        data = generate_records(num)
        kinesis_client = boto3.client('kinesis')
        for key,value in data.items():
            value_x = json.dumps(value)
            input = {'Data': value_x, 'PartitionKey': '1'}
            response = kinesis_client.put_records(
                StreamName=stream_name,
                Records=[input],
                )
            print(response)


    except:
        print("Error:", sys.exc_info()[0])
        raise


if __name__ == "__main__":
    main()