import json
import boto3
import datetime


def create_script(bucket, key):
    if 'event/' in key:
        print('event data arrived')
        event_job = '''from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sys
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

spark = SparkSession.builder.master("yarn").appName("PySpark").config(conf=SparkConf()).getOrCreate()

input_bucket = 's3://{}/'
input_path = '{}'

df = spark.read.load(input_bucket + input_path, format = 'csv', sep=",", inferschema = 'false', header = 'false')

df = df.withColumn("_c0", df['_c0'].cast(StringType())).withColumn("_c1", df['_c1'].cast(StringType())).withColumn("_c2", df['_c2'].cast(StringType())).withColumn("_c3", df['_c3'].cast(StringType())).withColumn("_c4", df['_c4'].cast(StringType())).withColumn("_c5", df['_c5'].cast(StringType())).withColumn("_c6", df['_c6'].cast(TimestampType())).withColumn("_c7", df['_c7'].cast(IntegerType())).withColumn("_c8", df['_c8'].cast(DecimalType(10,0)))

df = df.withColumnRenamed('_c0', "identity_adid").withColumnRenamed('_c1', "os").withColumnRenamed('_c2', "model").withColumnRenamed('_c3', "country").withColumnRenamed('_c4', "event_name").withColumnRenamed('_c5', "log_id").withColumnRenamed('_c6', "server_datetime").withColumnRenamed('_c7', "quantity").withColumnRenamed('_c8', "price")

from pyspark.sql.functions import year, month, dayofmonth

df_year_month_day = df.withColumn("year", year(df["server_datetime"]).cast("string")).withColumn("month", month(df["server_datetime"]).cast("string")).withColumn("day", dayofmonth(df["server_datetime"]).cast("string"))

output_bucket = "s3://aws-emr-toretto"
output_path = '/output/event_partitioned.parquet'

df_year_month_day.write.mode('append').partitionBy("year","month","day").parquet(output_bucket + output_path)

spark.stop()'''.format(bucket, key)
        encoded_script = event_job.encode('utf-8')
        Key = 'scripts/etl/event_job_{}.py'.format(key.replace('input/event/','').replace('/', '_'))
    
        data_type = 'event'
        print('well created {} job script'.format(data_type))
        return encoded_script, Key, data_type

    elif 'attribution/' in key:
        print('attribution data arrived')
        attribution_job = '''from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sys
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

spark = SparkSession.builder.master("yarn").appName("PySpark").config(conf=SparkConf()).getOrCreate()

input_bucket = 's3://{}/'
input_path = '{}'

att = spark.read.load(input_bucket + input_path, format = 'csv', sep=",", inferschema = "false", header = 'false')

att = att.withColumn("_c0", att['_c0'].cast(StringType())).withColumn("_c1", att['_c1'].cast(StringType())).withColumn('_c2', att['_c2'].cast(TimestampType())).withColumn("_c3", att['_c3'].cast(StringType())).withColumn("_c4", att['_c4'].cast(StringType())).withColumn("_c5", att['_c5'].cast(IntegerType())).withColumn("_c6", att['_c6'].cast(StringType()))

att = att.withColumnRenamed("_c0", 'partner').withColumnRenamed('_c1', "campaign").withColumnRenamed('_c2', "server_datetime").withColumnRenamed('_c3', "tracker_id").withColumnRenamed('_c4', "log_id").withColumnRenamed('_c5', "attribution_type").withColumnRenamed('_c6', "identity_adid")

from pyspark.sql.functions import year, month, dayofmonth

att_year_month_day = att.withColumn("year", year(att["server_datetime"]).cast("string")).withColumn("month", month(att["server_datetime"]).cast("string")).withColumn("day", dayofmonth(att["server_datetime"]).cast("string"))

output_bucket = "s3://aws-emr-toretto"
output_path = '/output/attribution_partitioned.parquet'

att_year_month_day.write.mode('append').partitionBy("year","month","day").parquet(output_bucket + output_path)

del att, att_year_month_day

spark.stop()'''.format(bucket, key)
    
    
        encoded_script = attribution_job.encode('utf-8')
        Key = 'scripts/etl/attribution_job_{}.py'.format(key.replace('input/attribution/','').replace('/', '_'))
        
        data_type = 'attribution'
        print('well created {} job script'.format(data_type))
        return encoded_script, Key, data_type
    else:
        print('data type error.. neither event nor attribution')
        return quit()

def upload_script(encoded_script, Key, bucket):
    return boto3.resource('s3').Bucket(bucket).put_object(Key = Key, Body = encoded_script)
    
def create_cluster(Key, data_type):
    if data_type == 'event':
        subnet_add = 'subnet-ee3681a3'
    else:
        subnet_add = 'subnet-28b5c741'

    connection = boto3.client(
        'emr',
        region_name='ap-northeast-2',
    )

    return connection.run_job_flow(
        Name='toretto-emr-boto3',
        LogUri='s3://aws-emr-toretto/logs/0_ETL_logs',
        ReleaseLabel='emr-5.30.0',
        Applications=[
            {
                'Name': 'Spark'
            },
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'c4.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'c4.xlarge',
                    'InstanceCount': 2,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': subnet_add,
        },
        Steps = [
            {
                'Name' : 'ETL',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                        'Args': [
                            'spark-submit',
                            '--deploy-mode', 'cluster',
                            '--conf', 'spark.yarn.submit.waitAppCompletion=true',
                            's3://aws-emr-toretto/{}'.format(Key)
                        ],
                        'Jar' : 'command-runner.jar'
                    }
        }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
    )


def write_log(bucket, data_type, key, cluster_id):
    try:
        response = boto3.client('s3').get_object(Bucket = bucket, Key = 'logs/0_data_logs/update_log.txt')
        update_log = response['Body'].read().decode('utf-8')
        print(update_log)
    
        temp = key.split('/')
        
        updated_log = update_log + "\n{},{},{},{},{},{}"\
            .format(data_type,temp[2],temp[3],temp[4],cluster_id,str(datetime.datetime.now()))
        print(updated_log)
        encoded_text = updated_log.encode('utf-8')
        
        print('getting object success !')
    except:
        print('getting object failed...')
    
    try:
        response = boto3.resource('s3').Bucket(bucket).put_object(Key = 'logs/0_data_logs/{}.txt'\
    .format(key.replace('input/','').replace('/','-')), Body = encoded_text)
    
        print("well uploaded result..! " + key.replace('input/{}/'.format(data_type),'').replace('/','-'))
    except:
        print("uploading result failed...")