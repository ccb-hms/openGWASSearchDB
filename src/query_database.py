import os
import sqlite3
import pandas as pd

__version__ = "0.4.0"


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
        m.SourceTermID AS 'OpenGWASID', 
        m.SourceTerm AS 'OpenGWASTrait',
        m.MappedTermLabel AS 'OntologyTerm', 
        m.MappedTermCURIE AS 'OntologyTermID',
        m.MappingScore AS 'MappingConfidence'
    FROM opengwas_mappings m
    LEFT JOIN efo_edges ee ON (m.MappedTermCURIE = ee.Subject)
    WHERE (m.MappedTermCURIE = 'EFO:0009605')

---
arguments:
    search_term='EFO:0009605'
    include_subclasses=True
    direct_subclasses_only=True

SQL:
    SELECT DISTINCT m.SourceTermID AS 'OpenGWASID', 
        m.SourceTerm AS 'OpenGWASTrait',
        m.MappedTermLabel AS 'OntologyTerm', 
        m.MappedTermCURIE AS 'OntologyTermID',
        m.MappingScore AS 'MappingConfidence'
    FROM opengwas_mappings m
    LEFT JOIN efo_edges ee ON (m.MappedTermCURIE = ee.Subject)
    WHERE (m.MappedTermCURIE = 'EFO:0009605' OR ee.Object = 'EFO:0009605')

---
arguments:
    search_term='EFO:0009605'
    include_subclasses=True
    direct_subclasses_only=False

SQL:
    SELECT DISTINCT m.SourceTermID AS 'OpenGWASID', 
        m.SourceTerm AS 'OpenGWASTrait',
        m.MappedTermLabel AS 'OntologyTerm', 
        m.MappedTermCURIE AS 'OntologyTermID',
        m.MappingScore AS 'MappingConfidence'
    FROM opengwas_mappings m
    LEFT JOIN efo_entailed_edges ee ON (m.MappedTermCURIE = ee.Subject)
    WHERE (m.MappedTermCURIE = 'EFO:0009605' OR ee.Object = 'EFO:0009605')
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
                    m.SourceTermID AS 'OpenGWASID', 
                    m.SourceTerm AS 'OpenGWASTrait',
                    m.MappedTermLabel AS 'OntologyTerm', 
                    m.MappedTermCURIE AS 'OntologyTermID',
                    m.MappingScore AS 'MappingConfidence'
                FROM opengwas_mappings m
                LEFT JOIN ''' + ontology_table + ''' ee ON (m.MappedTermCURIE = ee.Subject)
                WHERE (m.MappedTermCURIE = \'''' + search_term + '''\'''' + \
            (''' OR ee.Object = \'''' + search_term + '''\'''' if include_subclasses else '''''') +\
            ''')'''
    results = cursor.execute(query).fetchall()
    results_columns = [x[0] for x in cursor.description]
    results_df = pd.DataFrame(results, columns=results_columns)
    results_df = results_df.sort_values(by=['OpenGWASID'])
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
    db_connection = sqlite3.connect("../opengwas_search.db")
    db_cursor = db_connection.cursor()

    do_example_queries(db_cursor, search_term="EFO:0009605")  # 'pancreas disease'
    do_example_queries(db_cursor, search_term="EFO:0005741")  # 'infectious disease'
    do_example_queries(db_cursor, search_term="EFO:0004324")  # 'body weights and measures'

    db_cursor.close()
    db_connection.close()
