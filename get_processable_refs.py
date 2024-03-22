from pyspark.sql import SparkSession
import pandas
import awswrangler as wr
import json 
import sys
import re

input = sys.argv[1]
output = sys.argv[2]
JSONL_FILE_NAME = input
OUTPUT = output
WANTED_COLUMNS = ['DOI', 'type', 'title', 'author', 'member', 'prefix', 'reference-count', 'reference']


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
    ref_count, refs = 0, None
    for c in WANTED_COLUMNS:
        data[c] = contents.get(c, None)
    if data['reference']:
        ref_count, refs = get_processable_references(data['reference'])
    data['processable_ref_count'] = ref_count
    data['processable_references'] = refs
    return data


def process_record(contents):
    data = {}
    ref_count = 0
    if 'reference' in contents.keys():
        ref_count = len(contents['reference'])
    if ref_count > 25:
        data = get_refs(contents)
    return data

spark = SparkSession.builder \
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.8.5') \
            .getOrCreate()

all_data = spark.sparkContext.textFile(JSONL_FILE_NAME, minPartitions=1000)
all_data = all_data.map(lambda r: json.loads(r))
transformed_data = all_data.map(lambda d: process_record(d))
transformed_data = transformed_data.filter(lambda d: d)
transformed_data_df = spark.createDataFrame(transformed_data)
transformed_data_df.write.parquet(OUTPUT)