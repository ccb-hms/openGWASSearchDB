from owlready2 import *
from text2term import onto_utils
import pandas as pd

__version__ = "0.2.0"


class MappingReportGenerator:

    def __init__(self):
        pass

    def get_mapping_counts(self, mappings_df, ontology_iri, ontology_name):
        print("Getting counts for mappings to " + ontology_name + " (" + ontology_iri + ")...")
        ontology = get_ontology(ontology_iri).load()
        self.create_instances(ontology, ontology_name, mappings_df=mappings_df, use_reasoning=False)
        term_iri_column = "Mapped Term IRI"
        output = []
        for term in ontology.classes():
            if "BFO_" not in term.iri and "PATO_" not in term.iri:
                term_df = mappings_df[mappings_df[term_iri_column] == term.iri]
                direct_mappings = term_df.shape[0]
                instances = term.instances()
                local_instances = []
                for instance in instances:
                    if onto_utils.BASE_IRI in instance.iri:
                        local_instances.append(instance)
                inferred_mappings = len(local_instances)
                output.append((term.iri, direct_mappings, inferred_mappings))
        output_df = pd.DataFrame(data=output, columns=['iri', 'direct mappings', 'inferred mappings'])
        print("...done")
        return output_df

    def  create_instances(self, ontology, ontology_name, mappings_df, save_ontology=False, use_reasoning=False):
        with ontology:
            class table_id(Thing >> str):  # TODO: NHANES-specific
                pass
            class resource_id(Thing >> str):
                pass
        for index, row in mappings_df.iterrows():
            # table_id = row['Table']
            # term_id = row['Variable']  # TODO: NHANES-specific
            term = row['Source Term']
            term_id = row['Source Term ID']
            class_iri = row['Mapped Term IRI']
            ontology_class = IRIS[class_iri]

            # term_iri = onto_utils.BASE_IRI + table_id + "_" + variable_id  # TODO: NHANES-specific
            term_iri = onto_utils.BASE_IRI + term_id
            if IRIS[term_iri] is not None:
                labels = IRIS[term_iri].label
                if term not in labels:
                    labels.append(term)
            else:
                new_instance = ontology_class(label=term, iri=term_iri)  # create OWL instance to represent mapping
                new_instance.resource_id.append(term_id)
                # new_instance.table_id.append(table_id)  # TODO: NHANES-specific
        if save_ontology:
            ontology.save(ontology_name + "_mappings.owl")

        if use_reasoning:
            print("...reasoning over ontology...")
            owlready2.reasoning.JAVA_MEMORY = 20000  # TODO: even so the HermiT reasoner performs poorly on EFO+mappings
            with ontology:
                sync_reasoner()
