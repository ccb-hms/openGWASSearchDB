import os
import sys
import tarfile
import time
import ieugwaspy
import pandas as pd
from datetime import datetime

__version__ = "0.3.0"

# Versions of ontologies and the resulting search database
EFO_VERSION = "3.57.0"
UBERON_VERSION = "2023-07-25"
SEARCH_DB_VERSION = "0.10.0"

DATASET_NAME = "opengwas"
OUTPUT_DATABASE_FILEPATH = "../" + DATASET_NAME + "_search.db"


def delete_existing_resources():
    _delete_file("../resources/efo.db")
    _delete_file("../resources/efo_dbxrefs.tsv")
    _delete_file("../resources/efo_edges.tsv")
    _delete_file("../resources/efo_entailed_edges.tsv")
    _delete_file("../resources/efo_labels.tsv")
    _delete_file("../resources/efo_mappings_counts.tsv")
    _delete_file("../resources/opengwas_mappings.csv")
    _delete_file("../resources/opengwas_metadata.tsv")
    _delete_file("../resources/opengwas_references.tsv")
    _delete_file("../opengwas_search.db")


def _delete_file(file):
    if os.path.isfile(file):
        os.remove(file)


def get_version_info_table(metadata_timestamp):
    data = [("SearchDB", SEARCH_DB_VERSION),
            ("EFO", EFO_VERSION),
            ("UBERON", UBERON_VERSION),
            ("Metadata", metadata_timestamp)]
    df = pd.DataFrame(data, columns=["Resource", "Version"])
    return df


# Takes an optional argument that is an NCBI API KEY, which is used to query PubMed faster.
if __name__ == "__main__":
    delete_existing_resources()

    # Check if an NCBI API Key is provided
    if len(sys.argv) > 1:
        os.environ["NCBI_API_KEY"] = sys.argv[1]
        print(f"Using NCBI API Key: {os.environ.get('NCBI_API_KEY')}")
    else:
        print("NCBI API Key not provided—PubMed queries will be slower. Provide API Key as a parameter to this module.")

    # Fetch the OpenGWAS metadata directly from OpenGWAS using ieugwaspy package
    print("Downloading OpenGWAS metadata...")
    metadata_dict = ieugwaspy.gwasinfo()
    metadata_download_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    metadata_df = pd.DataFrame.from_dict(metadata_dict, orient="index")
    metadata_df = metadata_df[metadata_df["id"].str.contains("eqtl-a") == False]  # Remove eqtl records
    metadata_df["pmid"] = metadata_df["pmid"].astype(str).str.replace(".0", "", regex=False)  # remove '.0' from PMIDs
    metadata_df.to_csv("../resources/opengwas_metadata.tsv", index=False, sep="\t")

    version_info_df = get_version_info_table(metadata_timestamp=metadata_download_timestamp)

    # Build the database
    start = time.time()
    from build_database import build_database
    print("Building database...")
    build_database(dataset_name=DATASET_NAME,
                   metadata_df=metadata_df,
                   ontology_name="EFO",
                   ontology_url=f"http://www.ebi.ac.uk/efo/releases/v{EFO_VERSION}/efo.owl",
                   pmid_col="pmid",
                   resource_col="trait",
                   resource_id_col="id",
                   mapping_minimum_score=0.6,
                   include_cross_ontology_references_table=True,
                   output_database_filepath=OUTPUT_DATABASE_FILEPATH,
                   mapping_base_iris=("http://www.ebi.ac.uk/efo/", "http://purl.obolibrary.org/obo/MONDO",
                                      "http://purl.obolibrary.org/obo/HP", "http://www.orpha.net/ORDO",
                                      "http://purl.obolibrary.org/obo/DOID"),
                   additional_tables={"version_info": version_info_df},
                   additional_ontologies=["UBERON"])

    base_filename = os.path.basename(OUTPUT_DATABASE_FILEPATH)
    with tarfile.open(OUTPUT_DATABASE_FILEPATH + ".tar.xz", "w:xz") as tar:
        tar.add(OUTPUT_DATABASE_FILEPATH, arcname=base_filename)
    print(f"Finished building database ({time.time() - start:.1f} seconds)")
