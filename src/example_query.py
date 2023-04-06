import sqlite3
import pandas as pd

__version__ = "0.1.1"


def resources_annotated_with_term(cursor, search_term, include_subclasses=True, direct_subclasses_only=False):
    ontology_name = "efo"
    if include_subclasses:
        if direct_subclasses_only:
            ontology_table = ontology_name + "_edges"
        else:
            ontology_table = ontology_name + "_entailed_edges"
    else:
        ontology_table = ontology_name + "_edges"
    results = cursor.execute('''
                 SELECT DISTINCT m.`Source Term ID` AS "OpenGWAS ID", 
                    m.`Source Term` AS "OpenGWAS Trait",
                    m.`Mapped Term Label` AS "Ontology Term", m.`Mapped Term CURIE` AS "Ontology Term ID",
                    m.`Mapping Score` AS "Mapping Confidence"
                 FROM `opengwas_trait_mappings` m
                 LEFT JOIN ''' + ontology_table + ''' ee ON (m.`Mapped Term CURIE` = ee.subject)
                 WHERE (m.`Mapped Term CURIE` = \'''' + search_term + '''\'''' +
                             (''' OR ee.object = \'''' + search_term + '''\'''' if include_subclasses else '''''')
                             + ''') AND ee.predicate = 'rdfs:subClassOf'
                 ''').fetchall()
    results_columns = [x[0] for x in cursor.description]
    results_df = pd.DataFrame(results, columns=results_columns)
    results_df = results_df.sort_values(by=['OpenGWAS ID'])
    return results_df


def do_example_queries(cursor, search_term='EFO:0009605'):  # EFO:0009605 'pancreas disease'
    df1 = resources_annotated_with_term(cursor, search_term=search_term, include_subclasses=False)
    print("Resources annotated with " + search_term + ": " + ("0" if df1.empty else str(df1.shape[0])))
    if not df1.empty:
        print(df1.head().to_string() + "\n")

    df2 = resources_annotated_with_term(cursor, search_term=search_term, include_subclasses=True, direct_subclasses_only=True)
    print("Resources annotated with " + search_term + " or its direct (asserted) subclasses: " + ("0" if df2.empty else str(df2.shape[0])))
    if not df2.empty:
        print(df2.head().to_string() + "\n")

    df3 = resources_annotated_with_term(cursor, search_term=search_term, include_subclasses=True, direct_subclasses_only=False)
    print("Resources annotated with " + search_term + " or its indirect (inferred) subclasses: " + ("0" if df3.empty else str(df3.shape[0])))
    if not df3.empty:
        print(df3.head().to_string() + "\n")


if __name__ == '__main__':
    db_connection = sqlite3.connect("../opengwas_trait_search.db")
    db_cursor = db_connection.cursor()

    do_example_queries(db_cursor)                               # pancreas disease
    do_example_queries(db_cursor, search_term="EFO:0005741")    # infectious disease
    do_example_queries(db_cursor, search_term="EFO:0004324")    # body weights and measures

    db_cursor.close()
    db_connection.close()
