import json
import urllib.parse
import boto3
import ETL



def lambda_handler(event, context):
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    # create script
    encoded_script, Key, data_type = ETL.create_script(bucket, key)
    
    # upload script
    try:
        response = ETL.upload_script(encoded_script, Key, bucket)
        print('well uploaded {} job script'.format(data_type), Key)
    except:
        print('error to upload {} job script'.format(data_type))
    
    # create cluster

    response = ETL.create_cluster(Key, data_type)

    print ('cluster is creating now... ', response['JobFlowId'])

    # write info log
    c = response['JobFlowId']
    ETL.write_log(bucket, data_type, key, c)

    # lambda 호출시 payload
    payload = {'data_type' : data_type, 'Key' : key, 'cluster_id' : c}
    pay_json = json.dumps(payload)

    return pay_json
