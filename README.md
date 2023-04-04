# opengwas-search

`src/assemble_database.py` generates the SQLite3 database `opengwas_trait_search.db` that contains: 
- [text2term](https://github.com/ccb-hms/ontology-mapper)-generated mappings of traits in the OpenGWAS database to terms in the Experimental Factor Ontology (EFO)
- Tables that specify both asserted and inferred hierarchical (SubclassOf) relationships between terms in the EFO ontology, extracted from a [SemanticSQL](https://github.com/INCATools/semantic-sql) SQL build of EFO. 

By combining the software-generated mappings with these tables one can search over traits by leveraging the EFO class hierarchy. For example, to search for OpenGWAS records about any specific kind of 'respiratory disease'. 