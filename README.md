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


### Mapping phenotypes to EFO

The inputs to text2term are a table containing the OpenGWAS metadata from 2022-01-25 and the EFO ontology v3.43.0. We configured text2term to include only mappings with a score above our minimum threshold (`min_score=0.6`, in a [0,1] scale where 1=exact match), and to compute only the highest scored mapping for each trait in the metadata (`max_mappings=1`). We use the TFIDF mapper provided by text2term (`mapper=Mapper.TFIDF`), which computes TFIDF-based vector representations of traits and then uses cosine distance to determine how close each trait is to each ontology term (by considering ontology term labels and synonyms encoded in EFO). Finally we exclude terms that have been marked as deprecated (`excl_deprecated=True`) such that we only map to terms that are current and expected to be in EFO's future releases.

EFO contains terms and relationships between terms that exist in external ontologies such as MONDO, ChEBI, etc. Since our goal is to map phenotypes to appropriate terms in ontologies, if they exist, we further configured text2term to only map to terms from ontologies that describe phenotypes: EFO itself, the Monarch Disease Ontology (MONDO), the Human Phenotype Ontology (HPO), and the Orphanet Rare Disease Ontology (ORDO). To do this, we use the parameter `base_iris` in text2term which limits search to terms in the specified namespace(s), which we have set as follows: ('http://www.ebi.ac.uk/efo/', 'http://purl.obolibrary.org/obo/MONDO', 'http://purl.obolibrary.org/obo/HP',  'http://www.orpha.net/ORDO').

The text2term configuration is as follows:
```python
min_score=0.6,          # minimum acceptable mapping score  
mapper=Mapper.TFIDF,    # use the (default) TF-IDF-based mapper to compare strings  
excl_deprecated=True,   # exclude deprecated ontology terms
max_mappings=1,         # maximum number of mappings per input phenotype
base_iris=("http://www.ebi.ac.uk/efo/", 
           "http://purl.obolibrary.org/obo/MONDO",
           "http://purl.obolibrary.org/obo/HP", 
           "http://www.orpha.net/ORDO")
```
