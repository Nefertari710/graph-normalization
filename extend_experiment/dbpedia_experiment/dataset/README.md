# DBpedia Dataset

This directory contains four data files from the English DBpedia knowledge graph. In the filenames, `lang=en` indicates the English edition, `.ttl` indicates the RDF Turtle format, and `.bz2` indicates bzip2 compression. The data is organized as subject–property or relation–object triples.

| File | Contents | Main uses |
| --- | --- | --- |
| `instance-types_lang=en_specific.ttl.bz2` | Direct entity types expressed through `rdf:type`, such as an entity belonging to the `Band` or `Album` class. The `specific` version does not expand the full superclass inheritance chain. | Filtering entity classes and assigning node types. |
| `labels_lang=en.ttl.bz2` | English entity names expressed through `rdfs:label`; here, a label is a name rather than a classification label. | Displaying names, text search, and constructing text features. |
| `mappingbased-literals_lang=en.ttl.bz2` | Literal entity properties, such as aliases, years, dates, and numbers. | Constructing node properties and input features for embeddings. |
| `mappingbased-objects_lang=en.ttl.bz2` | Relations from entities to other resources, such as relations between a band and its members or musical genres. | Constructing graph edges and providing graph structure information. |

`mappingbased` indicates data extracted from Wikipedia infoboxes and represented consistently using DBpedia mapping rules. The objects in `literals` are concrete values, while those in `objects` are resource identifiers. The four files are linked through shared entity URIs.

## Raw Data Examples

The following examples show the first two lines from each decompressed file, preserving the original URIs. Each line is a triple with the following structure:

```text
<subject URI> <property or relation URI> object .
```

The object can be a resource URI or a literal. URIs are enclosed in `<...>`. Literals are enclosed in quotation marks and may include a language or datatype marker. The final `.` marks the end of the triple.

### 1. Entity Types: `instance-types_lang=en_specific.ttl.bz2`

```turtle
<http://dbpedia.org/resource/!!!> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Band> .
<http://dbpedia.org/resource/!!!_(album)> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Album> .
```

The structure is "entity → type relation `rdf:type` → class". The first line states that `!!!` is a band, and the second states that `!!!_(album)` is an album.

### 2. Entity Names: `labels_lang=en.ttl.bz2`

```turtle
<http://dbpedia.org/resource/!!!!!!!> <http://www.w3.org/2000/01/rdf-schema#label> "!!!!!!!"@en .
<http://dbpedia.org/resource/!!!> <http://www.w3.org/2000/01/rdf-schema#label> "!!!"@en .
```

The structure is "entity → name property `rdfs:label` → name text". `@en` indicates that the text is in English.

### 3. Property Values: `mappingbased-literals_lang=en.ttl.bz2`

```turtle
<http://dbpedia.org/resource/!!!> <http://dbpedia.org/ontology/activeYearsStartYear> "1996"^^<http://www.w3.org/2001/XMLSchema#gYear> .
<http://dbpedia.org/resource/!!!> <http://dbpedia.org/ontology/alias> "Chk Chk Chk"@en .
```

The structure is "entity → property → concrete value". The first line gives 1996 as the year activity began. The `gYear` datatype identifier after `^^` specifies that the value is a year. The second line gives `Chk Chk Chk` as the English alias.

### 4. Resource Relations: `mappingbased-objects_lang=en.ttl.bz2`

```turtle
<http://dbpedia.org/resource/!!!> <http://dbpedia.org/ontology/bandMember> <http://dbpedia.org/resource/Nic_Offer> .
<http://dbpedia.org/resource/!!!> <http://dbpedia.org/ontology/formerBandMember> <http://dbpedia.org/resource/Jerry_Fuchs> .
```

The structure is "entity → relation → another resource". The two lines link the band `!!!` to member `Nic_Offer` and former member `Jerry_Fuchs`, respectively. The objects are resource URIs rather than name strings.
