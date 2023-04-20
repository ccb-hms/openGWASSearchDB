import os
import sqlite3
import pandas as pd

__version__ = "0.2.0"


"""
This SQL query searches for OpenGWAS record identifiers and their associated traits, which we simply call "resources" 
for succinctness, based on mappings obtained by mapping the traits to the Experimental Factor Ontology (EFO) using text2term. 

    If include_subclasses=False, the query finds resources annotated with the given search term only.

    If include_subclasses=True, the query finds resources annotated with the given search term or its subclasses in the 
        ontology. This is done by including resources annotated with terms that are the 'subject' of a relationship 
        pair where the 'object' (i.e., parent) is the given search term. 
    
    If direct_subclasses_only=True, the query considers only the direct subclasses, given in the efo_edges table. 
    
    If direct_subclasses_only=False, the query considers all indirect, inferred subclasses, given in the efo_entailed_edges table.   
    
The SQL queries for all possible arguments are given below.

---
arguments:
    search_term='EFO:0009605'
    include_subclasses=False

SQL:
    SELECT DISTINCT 
        m.`Source Term ID` AS 'OpenGWAS ID', 
        m.`Source Term` AS 'OpenGWAS Trait',
        m.`Mapped Term Label` AS 'Ontology Term', 
        m.`Mapped Term CURIE` AS 'Ontology Term ID',
        m.`Mapping Score` AS 'Mapping Confidence'
    FROM `opengwas_trait_mappings` m
    LEFT JOIN `efo_edges` ee ON (m.`Mapped Term CURIE` = ee.subject)
    WHERE (m.`Mapped Term CURIE` = 'EFO:0009605')

---
arguments:
    search_term='EFO:0009605'
    include_subclasses=True
    direct_subclasses_only=True

SQL:
    SELECT DISTINCT m.`Source Term ID` AS 'OpenGWAS ID', 
        m.`Source Term` AS 'OpenGWAS Trait',
        m.`Mapped Term Label` AS 'Ontology Term', 
        m.`Mapped Term CURIE` AS 'Ontology Term ID',
        m.`Mapping Score` AS 'Mapping Confidence'
    FROM `opengwas_trait_mappings` m
    LEFT JOIN `efo_edges` ee ON (m.`Mapped Term CURIE` = ee.subject)
    WHERE (m.`Mapped Term CURIE` = 'EFO:0009605' OR ee.object = 'EFO:0009605')

---
arguments:
    search_term='EFO:0009605'
    include_subclasses=True
    direct_subclasses_only=False

SQL:
    SELECT DISTINCT m.`Source Term ID` AS 'OpenGWAS ID', 
        m.`Source Term` AS 'OpenGWAS Trait',
        m.`Mapped Term Label` AS 'Ontology Term', 
        m.`Mapped Term CURIE` AS 'Ontology Term ID',
        m.`Mapping Score` AS 'Mapping Confidence'
    FROM `opengwas_trait_mappings` m
    LEFT JOIN `efo_entailed_edges` ee ON (m.`Mapped Term CURIE` = ee.subject)
    WHERE (m.`Mapped Term CURIE` = 'EFO:0009605' OR ee.object = 'EFO:0009605')
"""


def resources_annotated_with_term(cursor, search_term, include_subclasses=True, direct_subclasses_only=False):
    """
    Retrieve resources annotated with the given search term and (optionally) subclasses of that term, by specifying
    include_subclasses=True. The argument direct_subclasses_only dictates whether to include only direct subclasses or
    all inferred/indirect subclasses
    :param cursor:  cursor for database connection
    :param search_term: the ontology term to search on
    :param include_subclasses:  include resources annotated with subclasses of the given search term,
        otherwise only resources explicitly annotated with that term are returned
    :param direct_subclasses_only:  include only the direct subclasses of the given search term,
        otherwise all the resources annotated with inferred subclasses of the given term are returned
    :return: data frame containing IDs and traits of the OpenGWAS records found to be annotated with the give term
    """
    if include_subclasses:
        if direct_subclasses_only:
            ontology_table = "efo_edges"
        else:
            ontology_table = "efo_entailed_edges"
    else:
        ontology_table = "efo_edges"

    query = '''SELECT DISTINCT 
                    m.`Source Term ID` AS 'OpenGWAS ID', 
                    m.`Source Term` AS 'OpenGWAS Trait',
                    m.`Mapped Term Label` AS 'Ontology Term', 
                    m.`Mapped Term CURIE` AS 'Ontology Term ID',
                    m.`Mapping Score` AS 'Mapping Confidence'
                FROM `opengwas_trait_mappings` m
                LEFT JOIN ''' + ontology_table + ''' ee ON (m.`Mapped Term CURIE` = ee.subject)
                WHERE (m.`Mapped Term CURIE` = \'''' + search_term + '''\'''' + \
            (''' OR ee.object = \'''' + search_term + '''\'''' if include_subclasses else '''''') +\
            ''')'''
    results = cursor.execute(query).fetchall()
    results_columns = [x[0] for x in cursor.description]
    results_df = pd.DataFrame(results, columns=results_columns)
    results_df = results_df.sort_values(by=['OpenGWAS ID'])
    return results_df


def do_example_query(cursor, search_term, include_subclasses, direct_subclasses_only):
    df = resources_annotated_with_term(cursor,
                                       search_term=search_term,
                                       include_subclasses=include_subclasses,
                                       direct_subclasses_only=direct_subclasses_only)
    print("Resources annotated with " + search_term + ": " + ("0" if df.empty else str(df.shape[0])))
    output_folder = "../test/example_query/"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_file = output_folder + "search_term_" + search_term
    if not df.empty:
        print(df.head().to_string() + "\n")
        if include_subclasses:
            if direct_subclasses_only:
                output_file += "_" + "incl_direct_subclasses"
            else:
                output_file += "_" + "incl_inferred_subclasses"
        df.to_csv(output_file + ".tsv", sep="\t", index=False)  # write out query results

    with open(output_file + ".txt", 'w') as f:  # write out query parameters and results count
        f.write("# query parameters:\n")
        f.write("search_term='%s'\n" % search_term)
        f.write("include_subclasses=%s\n" % include_subclasses)
        f.write("direct_subclasses_only=%s\n\n" % str(direct_subclasses_only))
        f.write("# query results count: %s" % str(df.shape[0]))
    return df


def do_example_queries(cursor, search_term='EFO:0009605'):  # EFO:0009605 'pancreas disease'
    do_example_query(cursor, search_term=search_term, include_subclasses=False, direct_subclasses_only=False)
    do_example_query(cursor, search_term=search_term, include_subclasses=True, direct_subclasses_only=True)
    do_example_query(cursor, search_term=search_term, include_subclasses=True, direct_subclasses_only=False)


if __name__ == '__main__':
    db_connection = sqlite3.connect("../opengwas_trait_search.db")
    db_cursor = db_connection.cursor()

    do_example_queries(db_cursor, search_term="EFO:0009605")
    """
    Expected output of search for 'pancreas disease':
    
    include_subclasses=False
        Resources annotated with EFO:0009605: 4
            PROT-a-2554  finn-a-K11_PANCOTH finn-b-E4_PANCSECNAS    finn-b-K11_PANCOTH
    
    include_subclasses=True, direct_subclasses_only=True    
        Resources annotated with EFO:0009605 or its direct (asserted) subclasses: 6
            PROT-a-2554 ebi-a-GCST90016675  ebi-a-GCST90016676  finn-a-K11_PANCOTH  finn-b-E4_PANCSECNAS
     
    include_subclasses=True, direct_subclasses_only=False    
        Resources annotated with EFO:0009605 or its indirect (inferred) subclasses: 109
            EBI-a-GCST005047    EBI-a-GCST005413    EBI-a-GCST005536    EBI-a-GCST005898    EBI-a-GCST006867
    """

    do_example_queries(db_cursor, search_term="EFO:0005741")
    """
    Expected output of search for 'infectious disease' 
    
    include_subclasses=False
        Resources annotated with EFO:0005741: 4
            finn-a-AB1_OTHER_INFECTIONS finn-b-AB1_OTHER_INFECTIONS finn-b-Z21_CARRIER_INFECTIOUS_DISEA finn-b-Z21_CONTACT_W_EXPOS_COMMUNICAB_DISEA
    
    include_subclasses=True, direct_subclasses_only=True
        Resources annotated with EFO:0005741 or its direct (asserted) subclasses: 58
            UKB-b-188   UKB-b-266   UKB-b-3683  finn-a-AB1_INFECTIONS finn-a-AB1_BACT_BIR_OTHER_INF_AGENTS ...
        
    include_subclasses=True, direct_subclasses_only=False
        Resources annotated with EFO:0005741 or its indirect (inferred) subclasses: 275
            PROT-a-2379 PROT-a-2455 PROT-a-627  PROT-a-737  UKB-a-112 ...
    """

    do_example_queries(db_cursor, search_term="EFO:0004324")
    """
    Expected output of search for 'body weights and measures':
    
    include_subclasses=False
        Resources annotated with EFO:0004324: 0
    
    include_subclasses=True, direct_subclasses_only=True
        Resources annotated with EFO:0004324 or its direct (asserted) subclasses: 66
            EBI-a-GCST004904    EBI-a-GCST006368    UKB-a-248   UKB-a-249   UKB-a-382 ...
            
    include_subclasses=True, direct_subclasses_only=False
        Resources annotated with EFO:0004324 or its indirect (inferred) subclasses: 118
            EBI-a-GCST003435    EBI-a-GCST004904    EBI-a-GCST005314    EBI-a-GCST006368    EBI-a-GCST007557 ...
    """

    db_cursor.close()
    db_connection.close()
