import os
import sqlite3
import time
import text2term
import bioregistry
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from metapub import PubMedFetcher
from generate_ontology_tables import get_semsql_tables_for_ontology
from generate_mapping_report import get_mapping_counts

__version__ = "1.2.3"

DB_RESOURCES_FOLDER = "../resources/"

text2term_mapping_source_term_col = "SourceTerm"
text2term_mapping_source_term_id_col = "SourceTermID"
text2term_mapping_target_term_iri_col = "MappedTermIRI"
text2term_mapping_score_col = "MappingScore"


# Assemble a SQLite database that contains:
# 1) The original user-specified metadata table
# 3) SemanticSQL tables of the specified ontology that enable search by leveraging the ontology class hierarchy
# 4) Mappings of the values in the specified column of the metadata table to terms in the specified ontology
# 5) Counts of how many data points in the metadata were mapped—either directly or indirectly—to each ontology term
def build_database(metadata_df, dataset_name, ontology_name,
                   resource_col=text2term_mapping_source_term_col,
                   resource_id_col=text2term_mapping_source_term_id_col,
                   ontology_term_iri_col=text2term_mapping_target_term_iri_col,
                   ontology_semsql_db_url="", ontology_url="", pmid_col="",
                   ontology_mappings_df=None, mapping_minimum_score=0.7, mapping_base_iris=()):
    ontology_name = ontology_name.lower()

    # Get target ontology URL from the specified ontology name
    if ontology_url == "":
        ontology_url = bioregistry.get_owl_download(ontology_name)

    # Create SQLite database
    db_name = "../" + dataset_name + "_search.db"
    Path(db_name).touch()
    db_connection = sqlite3.connect(db_name)

    # Add the given metadata table to the database
    import_df_to_db(db_connection, data_frame=metadata_df, table_name=dataset_name + "_metadata")

    # Get SemanticSQL ontology tables and add them to the database
    start = time.time()
    if ontology_semsql_db_url == "":
        ontology_semsql_db_url = "https://s3.amazonaws.com/bbop-sqlite/" + ontology_name + ".db"
    edges_df, entailed_edges_df, labels_df, dbxrefs_df, synonyms_df, ontology_version = get_semsql_tables_for_ontology(
        ontology_url=ontology_semsql_db_url,
        ontology_name=ontology_name.upper(),
        tables_output_folder=DB_RESOURCES_FOLDER,
        db_output_folder=DB_RESOURCES_FOLDER,
        save_tables=True)
    print(f"...done ({time.time() - start:.1f} seconds)")
    import_df_to_db(db_connection, data_frame=edges_df, table_name=ontology_name + "_edges")
    import_df_to_db(db_connection, data_frame=entailed_edges_df, table_name=ontology_name + "_entailed_edges")
    import_df_to_db(db_connection, data_frame=synonyms_df, table_name=ontology_name + "_synonyms")
    # TODO perhaps make inclusion of database cross-references optional
    # import_df_to_db(db_connection, data_frame=dbxrefs_df, table_name=ontology_name + "_dbxrefs")

    # Get details (title, abstract, journal) from PubMed about references in the specified PMID column
    references_table_filename = DB_RESOURCES_FOLDER + dataset_name + "_references.tsv"
    if not os.path.isfile(references_table_filename):
        references_df = get_pubmed_details(metadata_df=metadata_df, dataset_name=dataset_name, pmid_col=pmid_col)
    else:
        # TODO incrementally update the existing table with any new references in the metadata
        references_df = pd.read_csv(references_table_filename, sep="\t")
    import_df_to_db(db_connection, data_frame=references_df, table_name=dataset_name + "_references")

    # Map the values in the specified metadata table column to the specified ontology
    if ontology_mappings_df is None:
        ontology_mappings_df = map_metadata_to_ontologies(metadata_df=metadata_df, dataset_name=dataset_name,
                                                          ontology_url=ontology_url, min_score=mapping_minimum_score,
                                                          source_term_col=resource_col,
                                                          source_term_id_col=resource_id_col,
                                                          base_iris=mapping_base_iris)
        resource_col = text2term_mapping_source_term_col
        resource_id_col = text2term_mapping_source_term_id_col
        ontology_term_iri_col = text2term_mapping_target_term_iri_col
        ontology_mappings_df.columns = ontology_mappings_df.columns.str.replace(' ', '')
    import_df_to_db(db_connection, data_frame=ontology_mappings_df, table_name=dataset_name + "_mappings")

    # Get counts of mappings
    counts_df = get_mapping_counts(mappings_df=ontology_mappings_df, ontology_iri=ontology_url,
                                   source_term_col=resource_col, save_ontology=True,
                                   source_term_id_col=resource_id_col,
                                   mapped_term_iri_col=ontology_term_iri_col)
    counts_df.to_csv(DB_RESOURCES_FOLDER + ontology_name + "_mappings_counts.tsv", sep="\t", index=False)

    # Merge the counts table with the labels table on the "iri" column, and add the merged table to the database
    merged_df = pd.merge(labels_df, counts_df, on="IRI")
    import_df_to_db(db_connection, data_frame=merged_df, table_name=ontology_name + "_labels")


dtypes = {'int64': 'INTEGER', 'float64': 'REAL', 'object': 'TEXT', 'datetime64': 'TEXT'}


# Import the given data frame to the SQLite database through the specified connection
# The CREATE TABLE statement is built using the given data frame's column names and inferred data types
def import_df_to_db(connection, data_frame, table_name):
    columns = []
    for column_name, dtype in zip(data_frame.columns, data_frame.dtypes):
        sql_type = dtypes.get(str(dtype), 'TEXT')
        column_name = column_name.replace(":", "_")
        column_name = column_name.replace(" ", "")
        columns.append(f"`{column_name}` {sql_type}")
    create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(columns)})'
    connection.cursor().execute(create_table_query)
    data_frame.to_sql(table_name, connection, if_exists="replace", index=False)


# Map values in the specified metadata column to terms in the specified ontology set
def map_metadata_to_ontologies(metadata_df, dataset_name, ontology_url, min_score, source_term_col,
                               source_term_id_col, base_iris=()):
    print(f"Mapping values in metadata column '{source_term_col}' to terms in '{ontology_url}'...")
    start = time.time()
    source_terms = metadata_df[source_term_col].tolist()
    if source_term_id_col != "":
        source_term_ids = metadata_df[source_term_id_col].tolist()
    else:
        source_term_ids = ()
    mappings = text2term.map_terms(source_terms=source_terms, source_terms_ids=source_term_ids,
                                   target_ontology=ontology_url, excl_deprecated=True, save_graphs=False,
                                   max_mappings=1, min_score=min_score, save_mappings=True,
                                   output_file=DB_RESOURCES_FOLDER + dataset_name + "_mappings.csv",
                                   base_iris=base_iris)
    mappings.columns = mappings.columns.str.replace(" ", "")  # remove spaces from column names
    mappings[text2term_mapping_score_col] = mappings[text2term_mapping_score_col].astype(float).round(decimals=3)
    print(f"...done ({time.time() - start:.1f} seconds)")
    return mappings


# Get publication details from PubMed (title, abstract, journal, etc) for the PMIDS in the specified column
def get_pubmed_details(metadata_df, dataset_name, pmid_col):
    print("Fetching publication metadata from PubMed...")
    start = time.time()
    pmids = metadata_df[pmid_col].dropna().unique()
    fetch = PubMedFetcher()
    articles = []
    for pmid in tqdm(pmids):
        article_details = get_pubmed_article_details(fetch, pmid)
        if article_details != "":
            articles.append(article_details)
    references_df = pd.DataFrame(articles, columns=[pmid_col, 'Journal', 'Title', 'Abstract', 'Year', 'URL'])
    references_df.to_csv(DB_RESOURCES_FOLDER + dataset_name + "_references.tsv", sep="\t", index=False)
    print(f"...done ({time.time() - start:.1f} seconds)")
    return references_df


def get_pubmed_article_details(pubmed_fetcher, pmid):
    if pmid != "0" and pmid != "nan":
        try:
            article = pubmed_fetcher.article_by_pmid(pmid)
            title = article.title
            journal = article.journal
            year = article.year
            abstract = article.abstract
            url = article.url
            return pmid, journal, title, abstract, year, url
        except Exception as e:
            # Try to fetch details again. Sometimes the NCBI API errors out, but the second attempt (usually) succeeds
            return get_pubmed_article_details(pubmed_fetcher, pmid)
    return ""
