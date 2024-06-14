from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, IntegerType, StructField, StructType, ArrayType
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
import json
import sys
import re
from stopwordsiso import stopwords
import calendar
from datetime import datetime

input = sys.argv[1]
output = sys.argv[2]
INPUT = input
OUTPUT = output

def clean_refs(ref):
    clean_text = re.sub(r"doi\:.*?\s", "", ref)
    clean_text = re.sub(r"<.*?>", "", clean_text)
    clean_text = re.sub(r"\&.*?\;", "", clean_text)
    clean_text = re.sub(r"http.*?\s", "", clean_text)
    clean_text = re.sub(r"\d+", "", clean_text)
    clean_text = re.sub(r"_+", "", clean_text)
    clean_text = re.sub(r"\.*?", "", clean_text)
    clean_text = re.sub(r"\-.*?", "", clean_text)
    clean_text = re.sub(r"[\[|\]]", "", clean_text)
    return clean_text

def get_stopwords():
    months = [x.lower() for x in list(calendar.month_name)[1:]]
    eng_stopwords = stopwords("en")
    domain_stopwords = [
        "book",
        "last",
        "em",
        "accessed",
        "phys",
        "phd",
        "colloq",
        "univ",
        "college",
        "institute",
        "arxiv",
        "xxx",
        "review",
        "et al",
        "etal",
        "studies",
        "journal",
        "revue",
        "conference",
        "annals",
        "proceedings",
        "advances",
        "bulletin",
        "société",
        "del",
        "acta",
        "études",
        "tijdschrift",
        "voor",
        "anales",
        "журнал",
        "zeitschrift",
        "annales",
        "archiv",
        "thesis",
        "archive",
        "cited",
        "ref",
        "vol",
        "rev"
    ]
    all_stopwords = list(eng_stopwords) + list(stopwords("es")) + list(stopwords("de")) + list(stopwords("fr")) + list(stopwords("ru")) + domain_stopwords + months
    return all_stopwords

def get_max_word(doi, words):
    max_value = 0
    keys = list(words.keys())
    values = list(words.values())
    try:
        max_value = max(values)
        indices = list(np.where(np.array(values) == max(values)))[0]
        words = list(map(lambda x: keys[x], indices))
    except ValueError as ve:
        print(f"Error in max: for {doi}: word_dict: {words}. EXCEPTION : {ve.args[0]}")
    return [max_value, words]
    
def get_tokens(doi, refs):
    clean_text = {}
    tokens = {}
    all_tokens = []
    split_by = r'\w{2,15}'
    all_stopwords = get_stopwords() if 'unstructured' in refs.keys() else None
    for ref_type, reference_values in refs.items():
        tokens[ref_type] = []
        clean_text[ref_type] = [clean_refs(x) for x in reference_values]
        clean_text[ref_type] = [x for x in clean_text[ref_type] if re.findall(split_by,x)]
        for text in clean_text[ref_type]:
            tokens[ref_type].extend(re.findall(split_by, text))
        if ref_type == "unstructured":
            tokens[ref_type] = [t for t in tokens[ref_type] if t.lower() not in all_stopwords]
    cleaned_ref_count = sum(map(len,clean_text.values()))
    if tokens:
        for v in tokens.values():
            all_tokens.extend(v)
    return [all_tokens, cleaned_ref_count]

def count_tokens(doi, ref_length, tokens):
    words = {}
    for index, t in enumerate(tokens):
        keys = list(words.keys())
        if len(keys) == 0 or t not in keys:
            first_count = index
            words[t] = 1
        elif (t in keys) and (index != first_count):
            words[t] = words[t] + 1
    [value, calculated_words] = get_max_word(doi, words)
    frac_refs = value/ref_length
    return {"token_vocabulary": calculated_words, "token_frac_refs": frac_refs, "cleaned_references_length": ref_length}

def get_proc_refs_info(doi, refs):
    total_ref_length = sum(map(len,refs.values()))
    refs_info = {"DOI": doi, "token_vocabulary": None, "token_frac_refs": None,"total_processed_ref_len": total_ref_length, "cleaned_references_length": None}
    [tokens, cleaned_ref_count] = get_tokens(doi, refs)
    if tokens:
        word_count_info = count_tokens(doi, cleaned_ref_count, tokens)
        refs_info.update(word_count_info)
    return refs_info

def get_processable_references(reference):
    processable_references = {}
    authors = []
    unstructured = []
    for x in reference:
        if "unstructured" in x:
            # get the first 50 or fewer characters from unstructured
            cleaned = clean_refs(x["unstructured"][:50])
            unstructured.append(cleaned)
        elif "author" in x:
            authors.append(x["author"])
    if unstructured:
        unstructured = list(filter(lambda x: re.findall(r"\w{2,15}", x), unstructured))
        processable_references["unstructured"] = unstructured
    if authors:
        authors = list(
            filter(lambda x: re.findall(r"\w{2,15}", x), authors))
        processable_references["authors"] = authors
    processable_ref_count = len(authors) + len(unstructured)
    return processable_ref_count, processable_references

def get_authors(authors):
    authors = list(filter(lambda x: x, authors))
    authors = list(map(lambda x: x.lower(), authors))
    authors = ", ".join(authors)
    return authors

def get_refs(contents):
    data = {}
    proc_ref_count, processable_refs = get_processable_references(contents["reference"])
    if proc_ref_count >= 25:
        processable_ref_count_percentage = round(
            (proc_ref_count / contents["reference-count"]) * 100
        )
        if processable_ref_count_percentage >= 30:
            title = contents['title'][0] if 'title' in contents else None
            author = contents.get("author", None)
            authors = None
            if author:
                authors = [get_authors([x.get('family', None), x.get('given', None)]) for x in author]
            data = {
                "DOI": contents["DOI"],
                "type": contents.get("type", None),
                "author": authors,
                "title": title,
                "ref_count": contents.get("reference-count", None),
                "proc_ref_ptge": processable_ref_count_percentage,
                "proc_refs": processable_refs,
            }
    return data

def process_record(contents):
    data = {}
    ref_count = 0
    if "reference" in contents.keys():
        ref_count = len(contents["reference"])
    if ref_count > 25:
        data = get_refs(contents)
    return data

def author_flag(doi, vocabulary, authors):
    info = {"flag": "No"}
    if isinstance(vocabulary, list):
        process_authors = ",".join(authors)
        tokenized_authors = process_authors.split(",")
        tokenized_authors = [re.sub(r'\W', "", i) for i in tokenized_authors]
        for i in vocabulary:
            if i.lower() in tokenized_authors:
                info["flag"] = "Yes"
    else:
        print(f"Unexpected vocabulary: {vocabulary} for DOI: {doi}")
    return info

def get_values(row):
    doi = row["DOI"]
    refs = row["proc_refs"]
    v = get_proc_refs_info(doi, refs)
    v['work_type'] = row['type']
    v['author'] = row['author']
    v['title'] = row['title']
    v['proc_refs'] = row['proc_refs']
    v['flag'] = "No"
    if v['author']:
        tokens = v['token_vocabulary']
        flag = author_flag(doi, tokens, v['author'])
        v.update(flag)
    return v


def get_sub_dir(OUTPUT):
    now = datetime.now()
    dir_name = f"{OUTPUT}/{now.year}_{now.month}_{now.day}T{now.hour}_{now.minute}_{now.second}"
    return dir_name


spark = SparkSession.builder.config(
    "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.8.5"
).getOrCreate()

output_sub_dir = get_sub_dir(OUTPUT)

all_data = spark.sparkContext.textFile(
    "s3://outputs-private.research.crossref.org/snapshot-jsonl/snapshot-24-02/",
    minPartitions=1000,
)
'''
all_data = spark.sparkContext.textFile(
    "s3://outputs-private.research.crossref.org/snapshot-jsonl/snapshot-24-02/part0.jsonl",
    minPartitions=1000,
)'''
all_data = all_data.map(lambda r: json.loads(r))
transformed_data = all_data.map(lambda d: process_record(d))
transformed_data = transformed_data.filter(lambda d: d)
results = transformed_data.map(lambda r: get_values(r))
results = results.filter(
    lambda r: r["token_frac_refs"] and r["token_frac_refs"] >= 0.35
)

schema = StructType(
    [
        StructField("DOI", StringType(), False),
        StructField("token_vocabulary", ArrayType(StringType()), True),
        StructField("token_frac_refs", FloatType(), True),
        StructField("total_processed_ref_len", IntegerType(), True),
        StructField("cleaned_references_length", IntegerType(), True),
        StructField("work_type", StringType(), True),
        StructField("author", ArrayType(StringType()), True),
        StructField("flag", StringType(), True),
        StructField("title", StringType(), True)
    ]
)
token_count_dataframe = spark.createDataFrame(results, schema=schema)
parquet_filename = f"{output_sub_dir}.parquet"
token_count_dataframe.write.parquet(parquet_filename)
