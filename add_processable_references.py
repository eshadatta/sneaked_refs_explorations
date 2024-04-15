from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, StructField, StructType
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.sparse import issparse
import numpy as np
import json 
import sys
import re
from stopwordsiso import stopwords
import calendar
from datetime import datetime
import boto3

input = sys.argv[1]
output = sys.argv[2]
INPUT = input
OUTPUT = output

def get_stopwords():
    months = [x.lower() for x in list(calendar.month_name)[1:]]
    eng_stopwords = stopwords('en')
    domain_stopwords = ["book","review", "et al", "studies","journal","revue","conference", "annals","proceedings","advances","bulletin","société","del","acta","études","tijdschrift","voor","anales","журнал","zeitschrift","annales","archiv","archiv"]
    all_stopwords = list(eng_stopwords) + list(stopwords('es')) + list(stopwords('de')) + list(stopwords('fr')) + list(stopwords('ru')) + domain_stopwords + months
    return all_stopwords


def clean_text(doi, text):
    X = None
    all_stopwords = get_stopwords()
    clean_text = re.sub(r"doi\:.*?\s","",text[0])
    clean_text = re.sub(r'<.*?>','',clean_text)
    clean_text = re.sub(r"http.*?\s","",clean_text)
    clean_text = re.sub(r"\d+", "",clean_text)
    vectorizer = TfidfVectorizer(stop_words=all_stopwords)
    try:
        X = vectorizer.fit_transform([clean_text])
    except Exception as e:
        print(f"ERROR: {doi}. Exception: {e}")
    return [X, vectorizer, clean_text]

def write_statistics(data, path):
    session = boto3.Session()
    s3 = session.resource('s3')
    obj = s3.Object(path)
    obj.put(json.dumps(data))


def get_highest_tf_idf_vocab(result_idf, vectorizer):
    features = vectorizer.get_feature_names_out()
    # converting from sparse matrix, to numpy array
    # since there is only one document of all concatenated refs
    # getting the values from the first index
    ar_refs_idf = result_idf.toarray()[0]
    highest_value = float(max(ar_refs_idf))
    # getting all positions where the value is the highest value
    position = np.where(ar_refs_idf == highest_value)[0]
    # get vocabulary from features that match the position 
    vocab = list(map(lambda x: features[x], position))
    return {"vocabulary": vocab, "tf_idf_value": highest_value}

def get_proc_refs_info(doi, refs):
    refs1_info = {"DOI": doi, "vocabulary": None, "tf_idf_value": None}
    refs = [r[:50] for r in refs]
    process_refs = [" ".join(refs)]
    has_chars = re.findall(r"\w{2,10}",process_refs[0])
    match_ptge = (len(has_chars)/len(refs)) * 100
    if match_ptge >= 30:
        refs1_idf, vect1, cleaned1_txt = clean_text(doi, process_refs)
        if issparse(refs1_idf):
            tf_idf_values = get_highest_tf_idf_vocab(refs1_idf, vect1)
            refs1_info.update(tf_idf_values)
    return refs1_info

def clean_refs(ref):
    clean_text = re.sub(r"doi\:.*?\s","",ref)
    clean_text = re.sub(r'<.*?>','',clean_text)
    clean_text = re.sub(r"http.*?\s","",clean_text)
    clean_text = re.sub(r"\d+", "",clean_text)
    return clean_text

def get_processable_references(reference):
    processable_references = []
    for x in reference:
        if 'unstructured' in x:
            cleaned = clean_refs(x['unstructured'])
            processable_references.append(cleaned)
        elif 'author' in x:
            processable_references.append(x['author'])
    processable_references = list(filter(lambda x: re.findall(r'\w+',x), processable_references))
    processable_ref_count = len(processable_references)
    return processable_ref_count, processable_references

def get_refs(contents):
    data = {}
    proc_ref_count, processable_refs = get_processable_references(contents['reference'])
    if proc_ref_count >= 25:
        processable_ref_count_percentage = round((proc_ref_count/contents['reference-count']) * 100)
        if processable_ref_count_percentage >= 30:
            data = {"DOI": contents['DOI'], "type": contents.get('type', None), "ref_count": contents['reference-count'], "proc_ref_ptge": processable_ref_count_percentage, "proc_refs": processable_refs}
    return data

def process_record(contents):
    data = {}
    ref_count = 0
    if 'reference' in contents.keys():
        ref_count = len(contents['reference'])
    if ref_count > 25:
        data = get_refs(contents)
    return data

def get_values(row):
    doi = row['DOI']
    refs = row['proc_refs']
    v = get_proc_refs_info(doi, refs)
    return v

def get_sub_dir(OUTPUT):
    now = datetime.now()
    dir_name = f"{OUTPUT}/{now.year}_{now.month}_{now.day}T{now.hour}_{now.minute}_{now.second}"
    return dir_name

spark = SparkSession.builder \
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.8.5') \
            .getOrCreate()

output_sub_dir = get_sub_dir(OUTPUT)
statistics = f"{output_sub_dir}/counts.json"
all_data = spark.sparkContext.textFile("s3://outputs-private.research.crossref.org/snapshot-jsonl/snapshot-24-02/", minPartitions=1000)
all_data = all_data.map(lambda r: json.loads(r))
all_data_count = all_data.count()
transformed_data = all_data.map(lambda d: process_record(d))
transformed_data = transformed_data.filter(lambda d: d)
transformed_data_count = transformed_data.count()
transformed_data_df = spark.createDataFrame(transformed_data)
tf_idf_results = transformed_data.map(lambda r: get_values(r))
tf_idf_results = tf_idf_results.filter(lambda r: r['tf_idf_value'])
tf_idf_results = tf_idf_results.filter(lambda r: r['tf_idf_value'] >= 0.5)
tf_idf_results_count = tf_idf_results.count()
get_statistics = {"total_results_count": all_data_count, "number_of_unstructured_author_references_processed": transformed_data_count, "number_records_tf_idf_values": tf_idf_results_count}
schema = StructType([StructField("DOI", StringType(),False), StructField("vocabulary", StringType(),True), StructField("tf_idf_value", FloatType(),True)])
tf_idf_dataframe = spark.createDataFrame(tf_idf_results, schema=schema)
tf_idf_values = transformed_data_df.join(tf_idf_dataframe, 'DOI', "inner")
sorted_tf_idf_values = tf_idf_values.filter(tf_idf_values.tf_idf_value >= 0.75).sort("tf_idf_value", ascending=False).select("DOI", "vocabulary", "tf_idf_value")
sorted_tf_idf_values.write.option("header", True).csv(output_sub_dir)
tf_idf_values.write.format('json').save(f"{output_sub_dir}/all")
write_statistics(get_statistics, statistics)