from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, MapType, IntegerType, StructField, StructType, ArrayType
import numpy as np
import json
import sys
import re
from stopwordsiso import stopwords
import calendar
from datetime import datetime
from bs4 import BeautifulSoup


# get unnecessary tokens file
# get countries file - for consistency sake
input = sys.argv[1]
output = sys.argv[2]
ignore_tokens = sys.argv[3]
ignore_countries = sys.argv[4]
INPUT = input
OUTPUT = output

def read_file(f):
    data = {}
    try:
        with open(f, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print("ERROR: ", e)
    return data

def get_tokens_data():
    unwanted_tokens = read_file(ignore_tokens)
    stopword_countries = read_file(ignore_countries)
    unwanted_tokens = [x.lower() for x in unwanted_tokens['unwanted_tokens']]
    stopword_countries = [x.lower() for x in stopword_countries['countries']]
    stopword_tokens = list(set(unwanted_tokens)) + stopword_countries
    return stopword_tokens


def clean_refs(ref):
    clean_text = re.sub(r"doi\:.*?\s", "", ref)
    clean_text = re.sub(r"<.*?>", "", clean_text)
    clean_text = re.sub(r"\&.*?\;", "", clean_text)
    clean_text = re.sub(r"http\S+", "", clean_text)
    clean_text = re.sub(r"\d+", "", clean_text)
    clean_text = re.sub(r"_+", "", clean_text)
    clean_text = re.sub(r"\.*?", "", clean_text)
    clean_text = re.sub(r"\-.*?", "", clean_text)
    clean_text = re.sub(r"[\[|\]]", "", clean_text)
    return clean_text

def get_stopwords():
    months = [x.lower() for x in list(calendar.month_name)[1:]]
    eng_stopwords = stopwords("en")
    additional_stopwords = get_tokens_data()
    domain_stopwords = [
        "https",
        "book",
        "vgl",
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
        "rev",
        "magtechrefsourc", 
        "span",
        "referans",
        "title",
        "bibitem",
        "https",
        "reference",
        "class",
        "comatyponpdfplu", 
        "lusxmlimplaut", 
        "sinternalmodelp"
    ]
    all_stopwords = list(eng_stopwords) + list(stopwords("es")) + list(stopwords("it")) + list(stopwords("de")) + list(stopwords("fr")) + list(stopwords("ru")) + domain_stopwords + months + additional_stopwords
    return all_stopwords

def remove_common_author_strings():
    additional_stopwords = get_tokens_data()
    # these are common strings that seem to occur in author fields
    tokens = ["and", "vgl", "et", "al", "magtechrefsourc", "span","referans","title","bibitem","https","reference","class","comatyponpdfplu", "lusxmlimplaut", "sinternalmodelp"] + additional_stopwords
    return tokens

def get_max_word(words):
    # sorting dictionary of words and their reference count and token count in descending order
    sorted_word_dict = dict(sorted(words.items(), key=lambda item: (item[1]['reference_count'], item[1]['token_count']), reverse=True))
    # the highest reference count and token count will be the first n elements
    # in the following format:
    # [('a', {'token_count': 5, 'reference_count': 3}),
    #  ('test', {'token_count': 5, 'reference_count': 3}),
    #  ('str', {'token_count': 4, 'reference_count': 2})]
    process_sorted_word_list = list(sorted_word_dict.items())
    # getting the max token and ref count
    # it will be the first element as it is sorted by descending order
    max_token_count = process_sorted_word_list[0][1]['token_count']
    max_reference_count = process_sorted_word_list[0][1]['reference_count']
    max_occurring_word = []
    for el in process_sorted_word_list:
        if el[1]['token_count'] == max_token_count and el[1]['reference_count'] == max_reference_count:
            max_occurring_word.append(el[0])
    return max_occurring_word
    
def get_tokens(refs):
    clean_text = {}
    tokens = {}
    all_tokens = []
    split_by = r'\w{2,15}'
    all_stopwords = get_stopwords() if 'unstructured' in refs.keys() else None
    common_author_strings = remove_common_author_strings() if 'authors' in refs.keys() else None
    for ref_type, reference_values in refs.items():
        tokens[ref_type] = []
        clean_text[ref_type] = [clean_refs(x) for x in reference_values]
        clean_text[ref_type] = [x for x in clean_text[ref_type] if re.findall(split_by,x)]
        for text in clean_text[ref_type]:
            tokenized = re.findall(split_by, text)
            tokens[ref_type].extend([{"tokens": tokenized, "reference": text}])
            index = len(tokens[ref_type]) - 1
            if ref_type == "unstructured":
                tokens[ref_type][index]['tokens'] = [t for t in tokenized if t.lower() not in all_stopwords]
            elif ref_type == "authors":
                tokens[ref_type][index]['tokens'] = [t for t in tokenized if t.lower() not in common_author_strings]
    cleaned_ref_count = sum(map(len,clean_text.values()))
    if tokens:
        for v in tokens.values():
            all_tokens.extend(v)
    return [all_tokens, cleaned_ref_count]

def count_tokens(doi, ref_length, tokens):
    words = {}
    max_occurring_word = []
    frac_refs = 0.00
    for i in tokens:
        # this contains the token as a key and its count as the value
        token_count_info = {x: i['tokens'].count(x) for x in i['tokens']}
        counted_tokens = list(token_count_info.keys())
        keys = list(words.keys())
        existing_tokens = set(keys).intersection(counted_tokens)
        token_not_in_word_list = set(counted_tokens) - set(keys)
        if token_not_in_word_list:
            for t in token_not_in_word_list:
                words[t] = {"token_count": token_count_info[t], "reference_count": 1}
        if existing_tokens:
            for t in existing_tokens:
                words[t]['token_count'] = words[t]['token_count'] + token_count_info[t]
                words[t]['reference_count'] = words[t]['reference_count'] + 1
    if words: 
        max_occurring_word = get_max_word(words)
        reference_count = words[max_occurring_word[0]]['reference_count']
        frac_refs = reference_count/ref_length
    else:
        print(f"{doi} did not return any tokens")
    return {"token_vocabulary": max_occurring_word, "token_frac_refs": frac_refs, "cleaned_references_length": ref_length}

def get_proc_refs_info(doi, refs):
    total_ref_length = sum(map(len,refs.values()))
    refs_info = {"DOI": doi, "token_vocabulary": None, "token_frac_refs": None,"total_processed_ref_len": total_ref_length, "cleaned_references_length": None}
    [tokens, cleaned_ref_count] = get_tokens(refs)
    if tokens and cleaned_ref_count >= 25:
        word_count_info = count_tokens(doi, cleaned_ref_count, tokens)
        refs_info.update(word_count_info)
    return refs_info

def get_processable_references(reference):
    processable_references = {}
    authors = []
    unstructured = []
    for x in reference:
        if "unstructured" in x:
            removed_html_ref = BeautifulSoup(x["unstructured"]).get_text()
            # get the first 50 or fewer characters from unstructured
            cleaned = clean_refs(removed_html_ref[:50])
            unstructured.append(cleaned)
        elif "author" in x:
            removed_html_author = BeautifulSoup(x["author"]).get_text()
            authors.append(removed_html_author)
    if unstructured:
        unstructured = list(filter(lambda x: re.findall(r"\w{2,15}", x), unstructured))
        processable_references["unstructured"] = unstructured
    if authors:
        authors = list(
            filter(lambda x: re.findall(r"\w{2,15}", x), authors))
        processable_references["authors"] = authors
    processable_ref_count = len(authors) + len(unstructured)
    return processable_ref_count, processable_references

def get_processable_references2(reference):
    processable_references = {}
    authors = []
    unstructured = []
    for x in reference:
        if "unstructured" in x:
            removed_html_ref = BeautifulSoup(x["unstructured"]).get_text()
            # get the first 50 or fewer characters from unstructured
            cleaned = clean_refs(removed_html_ref[:50])
            unstructured.append(cleaned)
        elif "author" in x:
            removed_html_author = BeautifulSoup(x["author"]).get_text()
            authors.append(removed_html_author)
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

def get_refs(contents, ref_count):
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
            issn = contents['issn-type'] if 'issn-type' in contents else [{"value": None, "type": None}]
            data = {
                "DOI": contents["DOI"],
                "type": contents.get("type", None),
                "author": authors,
                "title": title,
                "issn": issn,
                "member": contents.get("member", None),
                "ref_count": contents.get("reference-count", None),
                "proc_ref_ptge": processable_ref_count_percentage,
                "proc_refs": processable_refs,
                "total_reference_length": ref_count
            }
    return data

def process_record(contents):
    data = {}
    ref_count = 0
    if "reference" in contents.keys():
        ref_count = len(contents["reference"])
    if ref_count > 25:
        data = get_refs(contents, ref_count)
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
    v['total_reference_length'] = row['total_reference_length']
    v['flag'] = "No"
    v['issn'] = row['issn']
    v['member'] = row['member']
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
    "s3://outputs-private.research.crossref.org/snapshot-jsonl/snapshot-24-06/",
    minPartitions=1000,
)

all_data = all_data.map(lambda r: json.loads(r))
transformed_data = all_data.map(lambda d: process_record(d))
transformed_data = transformed_data.filter(lambda d: d)
results = transformed_data.map(lambda r: get_values(r))
results = results.filter(
    lambda r: r["token_frac_refs"] and r["token_frac_refs"] >= 0.50
)

schema = StructType(
    [
        StructField("DOI", StringType(), False),
        StructField("token_vocabulary", ArrayType(StringType()), True),
        StructField("token_frac_refs", FloatType(), True),
        StructField("total_processed_ref_len", IntegerType(), True),
        StructField("cleaned_references_length", IntegerType(), True),
        StructField("total_reference_length", IntegerType(), False),
        StructField("work_type", StringType(), True),
        StructField("author", ArrayType(StringType()), True),
        StructField("flag", StringType(), True),
        StructField("member", StringType(), True),
        StructField("issn", ArrayType(MapType(StringType(), StringType(), True))),
        StructField("title", StringType(), True)
    ]
)
token_count_dataframe = spark.createDataFrame(results, schema=schema)
parquet_filename = f"{output_sub_dir}.parquet"
token_count_dataframe.write.parquet(parquet_filename)
