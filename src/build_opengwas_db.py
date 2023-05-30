import os
import ieugwaspy
import pandas as pd
from build_database import build_database


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


if __name__ == "__main__":
    delete_existing_resources()

    # Fetch the OpenGWAS metadata directly from OpenGWAS using ieugwaspy package
    metadata_dict = ieugwaspy.gwasinfo()
    metadata_df = pd.DataFrame.from_dict(metadata_dict, orient="index")
    metadata_df = metadata_df[metadata_df["id"].str.contains("eqtl-a") == False]  # Remove eqtl records
    metadata_df["pmid"] = metadata_df["pmid"].astype(str).str.replace(".0", "", regex=False)  # remove '.0' from PMIDs
    metadata_df.to_csv("../resources/opengwas_metadata.tsv", index=False, sep="\t")

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
