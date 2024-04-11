from pyspark.sql import SparkSession
import json 
import sys
from boto3  import client
import itertools 

input = sys.argv[1]
output = sys.argv[2]
INPUT = input
OUTPUT = output
bucket_name = "outputs-private.research.crossref.org"
prefix = "snapshot-jsonl/snapshot-24-02/"

def get_results(s3_conn, cont_key = None):
    if cont_key:
        s3_result =  s3_conn.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter = "/", ContinuationToken=cont_key)
    else:
        s3_result =  s3_conn.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter = "/")
    return s3_result

def get_files(results):
    objects = []
    for i in results:
        is_file = i['Key'].split("/")[-1]
        if is_file:
            objects.append(i['Key'])
    return objects

def get_objects(conn, cont_key = None, file_list = []):
    results = get_results(conn, cont_key)
    latest_files = get_files(results['Contents'])
    file_list.append(latest_files)
    if 'NextContinuationToken' in results:
        cont_key = results['NextContinuationToken']
        get_objects(conn, cont_key, file_list) 
    return file_list

def get_processable_references(reference):
    processable_references = []
    for x in reference:
        if 'author' in x:
            processable_references.append(x['author'])
        elif 'unstructured' in x:
            processable_references.append(x['unstructured'])
    processable_ref_count = len(processable_references)
    return processable_ref_count, processable_references

def get_refs(contents):
    data = {}
    proc_ref_count, processable_refs = get_processable_references(contents['reference'])
    processable_ref_count_percentage = round((proc_ref_count/contents['reference-count']) * 100)
    if processable_ref_count_percentage >= 30:
        data = {"DOI": contents['DOI'], "ref_count": contents['reference-count'], "proc_ref_ptge": processable_ref_count_percentage, "proc_refs": processable_refs}
    return data

def process_record(contents):
    data = {}
    ref_count = 0
    if 'reference' in contents.keys():
        ref_count = len(contents['reference'])
    if ref_count > 25:
        data = get_refs(contents)
    return data

s3_conn   = client('s3')
files = get_objects(s3_conn)
all_files = list(itertools.chain(*files))
s3_uri = f"s3://{bucket_name}"
full_s3_uris = list(map(lambda x: s3_uri + "/" + x, all_files))
files_list = ",".join(full_s3_uris)

spark = SparkSession.builder \
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.8.5') \
            .getOrCreate()
all_data = spark.sparkContext.textFile("s3://specificfile.jsonl", minPartitions=1000)
all_data = spark.sparkContext.textFile(files_list, minPartitions=1000)
all_data = all_data.map(lambda r: json.loads(r))
transformed_data = all_data.map(lambda d: process_record(d))
transformed_data = transformed_data.filter(lambda d: d)
transformed_data_df = spark.createDataFrame(transformed_data)
transformed_data_df.write.parquet(OUTPUT)

