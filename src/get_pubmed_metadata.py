import pandas as pd
from metapub import *

__version__ = "0.1.0"


def get_pubmed_details(pubmed_ids):
    fetch = PubMedFetcher()
    articles = []
    for pmid in pubmed_ids:
        if pmid != "0" and pmid != "nan":
            try:
                article = fetch.article_by_pmid(pmid)
                title = article.title
                journal = article.journal
                year = article.year
                abstract = article.abstract
                url = article.url
                articles.append((pmid, journal, title, abstract, year, url))
            except Exception as e:
                print("Unable to fetch article for PMID: " + str(pmid))
                print(e)

    articles_df = pd.DataFrame(articles, columns=['PMID', 'Journal', 'Title', 'Abstract', 'Year', 'URL'])
    return articles_df


if __name__ == "__main__":
    metadata_df = pd.read_csv("../resources/opengwas_metadata.tsv", sep="\t")
    pmids = metadata_df["pmid"].dropna().astype(str).str.replace(".0", "", regex=False)
    print("# PMIDs: " + str(len(pmids)))
    pmids = pmids.dropna().unique()
    print("# Unique PMIDS: " + str(len(pmids)))

    df = get_pubmed_details(pubmed_ids=pmids)
    df.to_csv("../resources/opengwas_references.tsv", sep="\t", index=False)
