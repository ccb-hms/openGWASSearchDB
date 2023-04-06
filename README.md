# opengwas-search

`src/assemble_database.py` generates the SQLite3 database `opengwas_trait_search.db` that contains:
- The original OpenGWAS metadata table with all traits and associated OpenGWAS DB record identifiers
- [text2term](https://github.com/ccb-hms/ontology-mapper)-generated mappings of OpenGWAS traits to Experimental Factor Ontology (EFO) terms
- Tables that specify asserted and inferred hierarchical (SubclassOf) relationships between EFO terms, extracted from a [SemanticSQL](https://github.com/INCATools/semantic-sql) EFO build. 

By combining the software-generated mappings with these tables one can search over traits by leveraging the EFO class hierarchy. 

`src/example_query.py` contains a simple function to query the generated database for OpenGWAS records related to a user-given trait. Executing this script will perform example queries for three traits and print the results. 

For example, when searching for OpenGWAS records about `pancreas disease` our approach finds more records than a keyword-based search over OpenGWAS traits:

![](resources/example_search_1.png)

Furthermore, using our approach, one can search for records about any more specific kind of `pancreas disease`, basically by including subclasses of 'pancreas disease' in the search, thus obtaining results such as:

![](resources/example_search_2.png)
