import os
import sys
import time
import ieugwaspy
import pandas as pd

__version__ = "0.2.0"


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
    metadata_dict = ieugwaspy.gwasinfo()
    metadata_df = pd.DataFrame.from_dict(metadata_dict, orient="index")
    metadata_df = metadata_df[metadata_df["id"].str.contains("eqtl-a") == False]  # Remove eqtl records
    metadata_df["pmid"] = metadata_df["pmid"].astype(str).str.replace(".0", "", regex=False)  # remove '.0' from PMIDs
    metadata_df.to_csv("../resources/opengwas_metadata.tsv", index=False, sep="\t")

    # Build the database
    start = time.time()
    from build_database import build_database
    build_database(dataset_name="opengwas",
                   metadata_df=metadata_df,
                   ontology_name="EFO",
                   pmid_col="pmid",
                   resource_col="trait",
                   resource_id_col="id",
                   mapping_minimum_score=0.6,
                   mapping_base_iris=("http://www.ebi.ac.uk/efo/", "http://purl.obolibrary.org/obo/MONDO",
                                      "http://purl.obolibrary.org/obo/HP", "http://www.orpha.net/ORDO",
                                      "http://purl.obolibrary.org/obo/DOID"))
    print(f"Finished building database ({time.time() - start:.1f} seconds)")
