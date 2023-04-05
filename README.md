# opengwas-search

`src/assemble_database.py` generates the SQLite3 database `opengwas_trait_search.db` that contains: 
- [text2term](https://github.com/ccb-hms/ontology-mapper)-generated mappings of traits in the OpenGWAS database to terms in the Experimental Factor Ontology (EFO)
- Tables that specify both asserted and inferred hierarchical (SubclassOf) relationships between terms in the EFO ontology, extracted from a [SemanticSQL](https://github.com/INCATools/semantic-sql) SQL build of EFO. 

By combining the software-generated mappings with these tables one can search over traits by leveraging the EFO class hierarchy. 

`src/example_query.py` contains a simple function that allows querying the generated database for OpenGWAS records related to a user-given trait. Executing this script will perform example queries for three traits and print the results. 

For example, when searching for OpenGWAS records about `pancreas disease` our approach finds more records than a keyword-based search over OpenGWAS traits:

![](resources/example_search_1.png)

Furthermore, using our approach, one can search for records about any more specific kind of `pancreas disease`, basically by including subclasses of 'pancreas disease' in the search, thus obtaining the following results:

![](resources/example_search_2.png)
