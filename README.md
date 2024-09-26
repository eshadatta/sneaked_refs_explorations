## Explorations in flagging sneaked references
**This is currently a work very much in progress.**

Based on the [pre-print](https://doi.org/10.48550/arXiv.2310.02192), _Sneaked references: Fabricated reference metadata distort citation counts_, that reported on “evidence of an undocumented method to manipulate citation counts” that involve “sneaked” references”, a document was generated in collaboration with the “research sleuths”. In it, several ideas were proposed in an attempt to flag this type of content; I took an approach that tried to focus only on Crossref metadata to see if we could flag data even if we did not have the actual content. I found several interesting aspects of the data worth highlighting, but using a metadata only approach, i.e. an approach that did not measure the references in the actual content along with the number of references in the metadata makes it **_difficult to definitively conclude whether there are sneaked references_**. However, it did highlight other potential issues that might also be interesting to investigate.

The basic algorithm for the metadata only approach was that the Crossref metadata snapshot was used to process metadata that had either unstructured references or authors and found the most occurring token(word) across those references.


## Workflow Summary
* To generate the data for this, I ran the script [add_processable_references.py](./add_processable_references.py) on Spark against a Crossref snapshot which is a dump of all its metadata at a certain date. 
    - The script processes snapshot data that had at least 25 references
    - It cleans the data data - removes stopwords in various languages, domain stopwords, as well as other random characters that leak through
    - Once it does that, it checks again to see if there are at least 25 references and at least 30% of those  references can be processed
    - From the articles, it gathers fields such as 
        - DOI, work type, author, title, container title, if ISSN exists, get that field, member ID, number of total references, number of references that can be used to process the maximum occurring token, percentage of token occurrence in references over all processed references, and the token itself

* The script outputs the data to a parquet file which is then downloaded to a local machine. 

* Based on the parquet file, there are various notebooks which output csv files based on various analyses.

## Workflow
* Run [add_processable_references.py](./add_processable_references.py) in Spark. 
    - The script requires the following arguments in order:
        - The path to the snapshot file, the path to the output directory, the path to a json list of tokens to be ignored, and the path to a list of country names
        - The script outputs a parquet file in the output directory listed as the second parameter
    - The past run was the done the following way:
    ```
    spark-submit --deploy-mode cluster s3://path/sneaked-references/add_processable_references.py s3://snapshot/path/snapshot-jsonl/snapshot-24-06/ s3://path/output/dir s3:/path/sneaked-references/unnecessary_tokens.json s3://path/sneaked-references/iso_country_names.json
    ``` 
* Download the parquet file to local machine
* Run the following notebooks to get outputs for various analyses
* The listed notebooks yield the following outputs:
    - [2024_08_19_analysis_journal_info.ipynb](./2024_08_19_analysis_journal_info.ipynb): This notebook generates a csv file that outputs journal level information with the highest number of DOIs that have one or more words that occur between 50% - 100% references. This means that the same single or few words has occurred in more than 50% of all references
    - [2024_08_19_analysis_journal_articles.ipynb](./2024_08_19_analysis_journal_articles.ipynb): Generates a csv file of all journal articles and a csv file of all journal articles which have duplicate titles
    - [2024_08_19_analysis_flagged_journal_articles.ipynb](./2024_08_19_analysis_flagged_journal_articles.ipynb): This notebook generates a dataframe of journal articles where the most common token could be one of the authors
    - [2024_08_19_analysis_unflagged_journal_articles.ipynb](./2024_08_19_analysis_unflagged_journal_articles.ipynb):  This notebook generates a csv of journal articles where the most common token is **not** one of the authors
    - [2024_08_19_analysis_book_chapters.ipynb](./2024_08_19_analysis_book_chapters.ipynb): This notebook generates a csv file of book chapters where the most common token is present between 50 - 100% of the references found in the chapters
    - [2024_08_19_analysis_books/ipynb](./2024_08_19_analysis_books.ipynb): This notebook generates a csv file of books where the most common token is present between 50 - 100% of the references found in its metadata
    - [2024_08_19_analysis_rem_work_types.ipynb](./2024_08_19_analysis_rem_work_types.ipynb): This notebook generates a csv file of various work types (proceedings articles, posted content, and other) where the most common token is present between 50 - 100% of the references found in its metadata
    - [cluster_similar_titles.ipynb](./cluster_similar_titles.ipynb): _Work in Progress_: This notebook needs to be cleaned up.

    ### This repo is still a work in progress and the approach and the code might change



