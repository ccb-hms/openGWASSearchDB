import os
import sqlite3
import text2term
import pandas as pd
from pathlib import Path
from text2term import Mapper
from generate_semql_ontology_tables import get_semsql_tables_for_ontology
from mapping_report_generator import MappingReportGenerator

__version__ = "0.2.3"


# Assemble a SQLite database that contains:
# 1) The original OpenGWAS metadata table containing all traits and associated OpenGWAS DB record identifiers
# 2) text2term-generated mappings of traits in the OpenGWAS database to terms in the Experimental Factor Ontology (EFO)
# 3) SemanticSQL tables of EFO that enable searching over traits by leveraging the EFO class hierarchy
def assemble_database(metadata_file):
    _clean_existing_resources()
    target_ontology_name = "EFO"
    # Get SemanticSQL EFO tables
    edges_df, entailed_edges_df, labels_df, dbxrefs_df, ontology_version = get_semsql_tables_for_ontology(
        ontology_url="https://s3.amazonaws.com/bbop-sqlite/efo.db",
        ontology_name=target_ontology_name,
        tables_output_folder="../resources/",
        db_output_folder="../resources/",
        save_tables=True)

    # Create SQLite database
    db_name = '../opengwas_trait_search.db'
    Path(db_name).touch()
    db_connection = sqlite3.connect(db_name)

    # Add OpenGWAS metadata table to the database
    metadata_tbl_cols = "idx,sex,category,population,group_name,build,author,year,trait,pmid,id,sample_size," \
                        "mr,nsnp,priority,ncase,ncontrol,ontology,subcategory,consortium,note,unit,access,batch," \
                        "units,sd,study_design,covariates,imputation_panel,qc_prior_to_upload,doi," \
                        "coverage,beta_transformation"
    import_df_to_db(db_connection, data_frame=pd.read_csv(metadata_file, dtype='unicode'),
                    table_name="opengwas_metadata", table_columns=metadata_tbl_cols)

    # Add SemanticSQL tables to the database
    semsql_tbl_cols = "subject,object"
    import_df_to_db(db_connection, data_frame=edges_df, table_name="efo_edges", table_columns=semsql_tbl_cols)
    import_df_to_db(db_connection, data_frame=entailed_edges_df, table_name="efo_entailed_edges", table_columns=semsql_tbl_cols)
    import_df_to_db(db_connection, data_frame=dbxrefs_df, table_name="efo_dbxrefs", table_columns=semsql_tbl_cols)

    # Use the same version of EFO as used in the SemanticSQL distribution of EFO
    efo_url = "https://github.com/EBISPOT/efo/releases/download/v" + ontology_version + "/efo.owl"

    # Map the traits to EFO and add the resulting mappings table to the database
    mappings = map_traits_to_efo(metadata_file, efo_url)
    import_df_to_db(db_connection, data_frame=mappings, table_name="opengwas_trait_mappings",
                    table_columns="`Source Term ID`,`Source Term`,`Mapped Term Label`,`Mapped Term CURIE`,"
                                  "`Mapped Term IRI`,`Mapping Score`,Tags,Ontology")

    # Get counts of mappings
    counts_df = MappingReportGenerator().get_mapping_counts(mappings_df=mappings,
                                                            ontology_name=target_ontology_name,
                                                            ontology_iri=efo_url)
    counts_df.to_csv("../resources/opengwas_efo_mappings_counts.tsv", sep="\t", index=False)

    # Merge the counts table with the labels table on the "iri" column, and add the merged table to the database
    merged_df = pd.merge(labels_df, counts_df, on="iri")
    import_df_to_db(db_connection, data_frame=merged_df, table_name="efo_labels", table_columns=semsql_tbl_cols + ",IRI")


# Import the given data frame to the SQLite database through the specified connection
def import_df_to_db(connection, data_frame, table_name, table_columns):
    create_table_query = '''CREATE TABLE IF NOT EXISTS ''' + table_name + ''' (''' + table_columns + ''')'''
    connection.cursor().execute(create_table_query)
    data_frame.to_sql(table_name, connection, if_exists='replace', index=False)


# Map traits in OpenGWAS metadata to terms in EFO
def map_traits_to_efo(metadata_file, ontology_url):
    return text2term.map_file(input_file=metadata_file, csv_columns=("trait", "id"), separator=",",
                              target_ontology=ontology_url, excl_deprecated=True, save_graphs=False,
                              max_mappings=1, min_score=0.6, mapper=Mapper.TFIDF,
                              save_mappings=True, output_file="../resources/opengwas_efo_mappings.csv",
                              base_iris=("http://www.ebi.ac.uk/efo/", "http://purl.obolibrary.org/obo/MONDO",
                                         "http://purl.obolibrary.org/obo/HP", "http://www.orpha.net/ORDO"))


def _clean_existing_resources():
    # _delete_file("../resources/efo.db")
    _delete_file("../resources/efo_edges.tsv")
    _delete_file("../resources/efo_entailed_edges.tsv")
    _delete_file("../resources/efo_dbxrefs.tsv")
    _delete_file("../resources/efo_labels.tsv")
    _delete_file("../resources/opengwas_efo_mappings.csv")
    _delete_file("../resources/opengwas_efo_mappings_counts.csv")
    _delete_file("../opengwas_trait_search.db")


def _delete_file(file):
    if os.path.isfile(file):
        os.remove(file)


if __name__ == "__main__":
    opengwas_metadata_20220125 = "../resources/opengwas_metadata_20220125.csv"  # TODO: fetch directly from OpenGWAS
    assemble_database(metadata_file=opengwas_metadata_20220125)
