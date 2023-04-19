import os
import sqlite3
import urllib.request
import pandas as pd

__version__ = "0.3.1"

ontologies = {
    "EFO": "https://s3.amazonaws.com/bbop-sqlite/efo.db",
    "FOODON": "https://s3.amazonaws.com/bbop-sqlite/foodon.db",
    "NCIT": "https://s3.amazonaws.com/bbop-sqlite/ncit.db"
}


def get_semsql_tables_for_ontologies(tables_output_folder='../ontology-tables',
                                     db_output_folder="../ontology-db",
                                     save_tables=False):
    all_edges = all_entailed_edges = all_labels = all_dbxrefs = pd.DataFrame()
    for ontology in ontologies:
        edges, entailed_edges, labels, dbxrefs, version = \
            get_semsql_tables_for_ontology(ontology_name=ontology,
                                           ontology_url=ontologies[ontology],
                                           db_output_folder=db_output_folder,
                                           save_tables=False)
        labels["ontology"] = edges["ontology"] = entailed_edges["ontology"] = dbxrefs["ontology"] = ontology
        all_labels = pd.concat([all_labels, labels])
        all_edges = pd.concat([all_edges, edges])
        all_entailed_edges = pd.concat([all_entailed_edges, entailed_edges])
        all_dbxrefs = pd.concat([all_dbxrefs, dbxrefs])

    if save_tables:
        save_table(all_labels, "ontology_labels.tsv", tables_output_folder)
        save_table(all_edges, "ontology_edges.tsv", tables_output_folder)
        save_table(all_entailed_edges, "ontology_entailed_edges.tsv", tables_output_folder)
        save_table(all_dbxrefs, "ontology_db_cross_references.tsv", tables_output_folder)
    return all_edges, all_entailed_edges, all_labels, all_dbxrefs


def get_semsql_tables_for_ontology(ontology_url, ontology_name, tables_output_folder='../ontology-tables',
                                   db_output_folder="../ontology-db", save_tables=False):
    db_file = os.path.join(db_output_folder, ontology_name.lower() + ".db")
    if not os.path.isfile(db_file):
        if not os.path.exists(db_output_folder):
            os.makedirs(db_output_folder)
        print("Downloading database file for " + ontology_name + "...")
        urllib.request.urlretrieve(ontology_url, db_file)
    print("Generating tables for " + ontology_name + "...")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    # Get the tables from the sqlite database
    edges_df = _get_edges_table(cursor)
    entailed_edges_df = _get_entailed_edges_table(cursor)
    labels_df = _get_labels_table(cursor)
    dbxrefs_df = _get_db_cross_references_table(cursor)
    onto_version = _get_ontology_version(cursor)
    cursor.close()
    conn.close()
    if save_tables:
        save_table(labels_df, ontology_name.lower() + "_labels.tsv", tables_output_folder)
        save_table(edges_df, ontology_name.lower() + "_entailed_edges.tsv", tables_output_folder)
        save_table(entailed_edges_df, ontology_name.lower() + "_edges.tsv", tables_output_folder)
        save_table(dbxrefs_df, ontology_name.lower() + "_dbxrefs.tsv", tables_output_folder)
    return edges_df, entailed_edges_df, labels_df, dbxrefs_df, onto_version


def _get_ontology_version(cursor):
    cursor.execute("SELECT `value` FROM statements WHERE predicate='owl:versionInfo'")
    ontology_version = cursor.fetchall()
    if len(ontology_version) > 0:
        return ontology_version.pop()[0]
    return ""


def _get_edges_table(cursor):
    cursor.execute("SELECT * FROM edge WHERE predicate='rdfs:subClassOf'")
    edge_columns = [x[0] for x in cursor.description]
    edge_data = cursor.fetchall()
    edges_df = pd.DataFrame(edge_data, columns=edge_columns)
    edges_df = edges_df.drop(columns=["predicate"])
    return edges_df


def _get_entailed_edges_table(cursor):
    cursor.execute("SELECT * FROM entailed_edge WHERE predicate='rdfs:subClassOf'")
    entailed_edge_columns = [x[0] for x in cursor.description]
    entailed_edge_data = cursor.fetchall()
    entailed_edges_df = pd.DataFrame(entailed_edge_data, columns=entailed_edge_columns)
    entailed_edges_df = entailed_edges_df.drop(columns=["predicate"])
    return entailed_edges_df


def _get_labels_table(cursor):
    # Get rdfs:label statements for ontology classes
    cursor.execute("SELECT * FROM statements WHERE predicate='rdfs:label' AND subject IN "
                   "(SELECT subject FROM statements WHERE predicate='rdf:type' AND object='owl:Class')")
    labels_columns = [x[0] for x in cursor.description]
    labels_data = cursor.fetchall()
    labels_df = pd.DataFrame(labels_data, columns=labels_columns)
    labels_df = labels_df.drop(columns=["stanza", "predicate", "object", "datatype", "language"])
    labels_df = labels_df[labels_df["subject"].str.startswith("_:") == False]  # remove blank nodes
    labels_df = labels_df.rename(columns={'value': 'object'})  # rename label value column to the same as other tables
    return labels_df


def _get_db_cross_references_table(cursor):
    cursor.execute("SELECT * FROM has_dbxref_statement")
    db_xrefs_columns = [x[0] for x in cursor.description]
    db_xrefs_data = cursor.fetchall()
    db_xrefs = pd.DataFrame(db_xrefs_data, columns=db_xrefs_columns)
    db_xrefs = db_xrefs.drop(columns=["stanza", "predicate", "object", "datatype", "language"])
    db_xrefs = db_xrefs[db_xrefs["subject"].str.startswith("_:") == False]  # remove blank nodes
    db_xrefs = db_xrefs.rename(columns={'value': 'object'})  # rename dbxref value column to the same as other tables
    return db_xrefs


def save_table(df, output_filename, tables_output_folder):
    if not os.path.exists(tables_output_folder):
        os.makedirs(tables_output_folder)
    output_file = os.path.join(tables_output_folder, output_filename)
    df.to_csv(output_file, index=False, sep="\t")


if __name__ == "__main__":
    get_semsql_tables_for_ontologies(save_tables=True)
