from pyspark.sql import SparkSession
import json 
import sys


input = sys.argv[1]
output = sys.argv[2]
INPUT = input
OUTPUT = output

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

spark = SparkSession.builder \
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.8.5') \
            .getOrCreate()

all_data = spark.sparkContext.textFile("s3://outputs-private.research.crossref.org/snapshot-jsonl/snapshot-24-02/part0.jsonl", minPartitions=1000)
all_data = all_data.map(lambda r: json.loads(r))
transformed_data = all_data.map(lambda d: process_record(d))
transformed_data = transformed_data.filter(lambda d: d)
transformed_data_df = spark.createDataFrame(transformed_data)
transformed_data_df.write.parquet(OUTPUT)