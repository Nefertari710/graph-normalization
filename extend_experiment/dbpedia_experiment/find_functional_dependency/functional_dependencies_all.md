# DBpedia node-property functional dependencies

Search status: **completed**. Classes scanned: 414; excluded by thresholds/limits: 25; pending: 0; failed: 0; single-RHS dependencies: 27; 1→N dependencies: 4.
Last saved (UTC): 2026-09-11T04:28:58.526723+00:00

Relational view: each explicit class is a relation, each node instance is one row, and each RDF literal predicate is an attribute.
Search domain: node-internal RDF literal properties in all selected explicit classes, including non-DBpedia namespaces; single-property X → Y and X → {Y1, ..., Yn}.
Outgoing relationships, node URI, RDF type, labels, and import metadata are not FD candidate fields.
Here 1→N means one determinant property determines N dependent properties; it is not a graph one-to-many relationship.
A multi-RHS dependency uses nodes where X and every Y are present and single-valued.
Multi-RHS candidates combine qualifying single-RHS dependencies with the same X, then recheck their common population.
Every listed Y must vary and must not merely repeat X in that common population; only maximal qualifying RHS sets are shown.
Untyped entities and composite determinant properties are outside this class-scoped search.
Thresholds: support >= 20; coverage >= 20%; repeated X groups >= 3. Names/metadata and trivial dependencies are excluded.
Limits (0 = unlimited): single-RHS total=0, class size=0, per class for each result kind=0. Both qualifying directions are retained.
Scope: each result checks every instance in its class with exactly one RDF literal in every participating property.
Missing and multivalued instances are excluded and counted. No sampling is used.
These are observed data dependencies, not guaranteed ontology/business rules.
Import complete at start: True; latest observation: True.
Post-import RDF verification recorded at start: False; latest observation: False.
Import completion records finished writes; RDF verification is a separate status.
Classes use explicit labels only (no subclass inference). Literal comparison preserves datatype/language.
Concurrent writes are not blocked. These results are observations, not an isolated database snapshot.
Rerun the search after any data changes to refresh the observed dependencies.

## One determinant → multiple dependent fields

Each row uses one common eligible population for X and every listed Y.

| # | Class | X → {Y1, …, Yn} | N | Eligible / class | Coverage | Repeated X groups |
|---|---|---|---:|---:|---:|---:|
| M1 | SnookerWorldRanking | birthDate → {activeYearsStartYear, highestRank} | 2 | 87 / 87 | 100.0% | 15 |
| M2 | Drug | fdaUniiCode → {chEMBL, drugbank} | 2 | 1,899 / 7,492 | 25.3% | 4 |
| M3 | Drug | fdaUniiCode → {chEBI, chEMBL} | 2 | 1,601 / 7,492 | 21.4% | 3 |
| M4 | Drug | casNumber → {chEBI, chEMBL} | 2 | 1,532 / 7,492 | 20.4% | 3 |

### M1. SnookerWorldRanking: birthDate → {activeYearsStartYear, highestRank}

Class: `http://dbpedia.org/ontology/SnookerWorldRanking`

X (literal): `http://dbpedia.org/ontology/birthDate`

Common scope: `whole_class`; eligible instances: 87 / 87; conflicting X groups: 0.

Dependent fields:

- activeYearsStartYear (literal): `http://dbpedia.org/ontology/activeYearsStartYear`
- highestRank (literal): `http://dbpedia.org/ontology/highestRank`

Example: X=`{"value": "1969-01-13", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → {activeYearsStartYear={"value": "1985", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}, highestRank={"value": "1", "datatype": "http://www.w3.org/2001/XMLSchema#integer", "language": null}} (15 instances share this X).

- 1990__No._3:_Stephen_Hendry__1: `http://dbpedia.org/resource/Snooker_world_rankings_1989/1990__No._3:_Stephen_Hendry__1`
- Snooker world rankings 1990/1991: `http://dbpedia.org/resource/Snooker_world_rankings_1990/1991`
- Snooker world rankings 1991/1992: `http://dbpedia.org/resource/Snooker_world_rankings_1991/1992`

Example: X=`{"value": "1957-08-22", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → {activeYearsStartYear={"value": "1978", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}, highestRank={"value": "1", "datatype": "http://www.w3.org/2001/XMLSchema#integer", "language": null}} (9 instances share this X).

- 1982__No._2:_Steve_Davis__1: `http://dbpedia.org/resource/Snooker_world_rankings_1981/1982__No._2:_Steve_Davis__1`
- Snooker world rankings 1983/1984: `http://dbpedia.org/resource/Snooker_world_rankings_1983/1984`
- Snooker world rankings 1987/1988: `http://dbpedia.org/resource/Snooker_world_rankings_1987/1988`

### M2. Drug: fdaUniiCode → {chEMBL, drugbank}

Class: `http://dbpedia.org/ontology/Drug`

X (literal): `http://dbpedia.org/ontology/fdaUniiCode`

Common scope: `complete_single_valued_subset`; eligible instances: 1,899 / 7,492; conflicting X groups: 0.

Dependent fields:

- chEMBL (literal): `http://dbpedia.org/ontology/chEMBL`
- drugbank (literal): `http://dbpedia.org/ontology/drugbank`

Example: X=`{"value": "1JQS135EYN", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → {chEMBL={"value": "395429", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}, drugbank={"value": "DB00107", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}} (2 instances share this X).

- Oxytocin: `http://dbpedia.org/resource/Oxytocin`
- Oxytocin (medication): `http://dbpedia.org/resource/Oxytocin_(medication)`

Example: X=`{"value": "P6YC3EG204", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → {chEMBL={"value": "2110563", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}, drugbank={"value": "DB00115", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}} (2 instances share this X).

- Cyanocobalamin: `http://dbpedia.org/resource/Cyanocobalamin`
- Vitamin B12: `http://dbpedia.org/resource/Vitamin_B12`

### M3. Drug: fdaUniiCode → {chEBI, chEMBL}

Class: `http://dbpedia.org/ontology/Drug`

X (literal): `http://dbpedia.org/ontology/fdaUniiCode`

Common scope: `complete_single_valued_subset`; eligible instances: 1,601 / 7,492; conflicting X groups: 0.

Dependent fields:

- chEBI (literal): `http://dbpedia.org/ontology/chEBI`
- chEMBL (literal): `http://dbpedia.org/ontology/chEMBL`

Example: X=`{"value": "1JQS135EYN", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → {chEBI={"value": "7872", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}, chEMBL={"value": "395429", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}} (2 instances share this X).

- Oxytocin: `http://dbpedia.org/resource/Oxytocin`
- Oxytocin (medication): `http://dbpedia.org/resource/Oxytocin_(medication)`

Example: X=`{"value": "X4W3ENH1CV", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → {chEBI={"value": "18357", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}, chEMBL={"value": "1437", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}} (2 instances share this X).

- Norepinephrine: `http://dbpedia.org/resource/Norepinephrine`
- Norepinephrine (medication): `http://dbpedia.org/resource/Norepinephrine_(medication)`

### M4. Drug: casNumber → {chEBI, chEMBL}

Class: `http://dbpedia.org/ontology/Drug`

X (literal): `http://dbpedia.org/ontology/casNumber`

Common scope: `complete_single_valued_subset`; eligible instances: 1,532 / 7,492; conflicting X groups: 0.

Dependent fields:

- chEBI (literal): `http://dbpedia.org/ontology/chEBI`
- chEMBL (literal): `http://dbpedia.org/ontology/chEMBL`

Example: X=`{"value": "11000-17-2", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → {chEBI={"value": "9937", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}, chEMBL={"value": "373742", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}} (2 instances share this X).

- Vasopressin: `http://dbpedia.org/resource/Vasopressin`
- Vasopressin (medication): `http://dbpedia.org/resource/Vasopressin_(medication)`

Example: X=`{"value": "50-56-6", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → {chEBI={"value": "7872", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}, chEMBL={"value": "395429", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}} (2 instances share this X).

- Oxytocin: `http://dbpedia.org/resource/Oxytocin`
- Oxytocin (medication): `http://dbpedia.org/resource/Oxytocin_(medication)`

## Single-dependent-field evidence

| # | Class | X → Y | Eligible / class | Coverage | Repeated X groups |
|---|---|---|---|---|---|
| 1 | SnookerWorldRanking | birthDate → activeYearsStartYear | 87 / 87 | 100.0% | 15 |
| 2 | SnookerWorldRanking | birthDate → highestRank | 87 / 87 | 100.0% | 15 |
| 3 | Rocket | finalFlight → diameter | 98 / 354 | 27.7% | 3 |
| 4 | PopulatedPlace | areaCode → utcOffset | 344 / 547 | 62.9% | 7 |
| 5 | RailwayStation | openingDate → openingYear | 589 / 1,967 | 29.9% | 80 |
| 6 | Philosopher | birthDate → birthYear | 1,796 / 2,849 | 63.0% | 37 |
| 7 | Philosopher | deathDate → deathYear | 1,178 / 2,849 | 41.3% | 20 |
| 8 | Play | premiereDate → premiereYear | 1,421 / 3,570 | 39.8% | 38 |
| 9 | Actor | birthDate → birthYear | 1,933 / 3,761 | 51.4% | 33 |
| 10 | Actor | deathDate → deathYear | 927 / 3,761 | 24.6% | 13 |
| 11 | PublicTransitSystem | openingDate → openingYear | 779 / 3,884 | 20.1% | 32 |
| 12 | Saint | deathDate → deathYear | 2,382 / 5,040 | 47.3% | 73 |
| 13 | Saint | birthDate → birthYear | 1,423 / 5,040 | 28.2% | 10 |
| 14 | Bridge | openingDate → openingYear | 1,334 / 5,693 | 23.4% | 46 |
| 15 | RailwayLine | openingDate → openingYear | 2,063 / 6,857 | 30.1% | 179 |
| 16 | Drug | fdaUniiCode → chEMBL | 3,785 / 7,492 | 50.5% | 5 |
| 17 | Drug | casNumber → chEMBL | 3,705 / 7,492 | 49.5% | 5 |
| 18 | Drug | fdaUniiCode → drugbank | 2,248 / 7,492 | 30.0% | 7 |
| 19 | Drug | fdaUniiCode → chEBI | 1,820 / 7,492 | 24.3% | 3 |
| 20 | Drug | casNumber → chEBI | 1,753 / 7,492 | 23.4% | 3 |
| 21 | Drug | chEMBL → chEBI | 1,663 / 7,492 | 22.2% | 4 |
| 22 | Dam | buildingStartDate → buildingStartYear | 2,815 / 7,776 | 36.2% | 141 |
| 23 | GovernmentAgency | formationDate → formationYear | 3,814 / 9,506 | 40.1% | 461 |
| 24 | PoliticalParty | formationDate → formationYear | 5,083 / 12,603 | 40.3% | 560 |
| 25 | Station | openingDate → openingYear | 23,366 / 52,929 | 44.1% | 3,877 |
| 26 | Person | birthDate → birthYear | 210,671 / 298,492 | 70.6% | 44,517 |
| 27 | Person | deathDate → deathYear | 115,303 / 298,492 | 38.6% | 26,346 |

## 1. SnookerWorldRanking: birthDate → activeYearsStartYear

Class: `http://dbpedia.org/ontology/SnookerWorldRanking`

X (literal): `http://dbpedia.org/ontology/birthDate`

Y (literal): `http://dbpedia.org/ontology/activeYearsStartYear`

Scope: `whole_class`; excluded instances: 0; conflicting X groups: 0.

- X: 0 missing; 0 multivalued.
- Y: 0 missing; 0 multivalued.

Example: `{"value": "1969-01-13", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1985", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (15 instances share this X).

- 1990__No._3:_Stephen_Hendry__1: `http://dbpedia.org/resource/Snooker_world_rankings_1989/1990__No._3:_Stephen_Hendry__1`
- Snooker world rankings 1990/1991: `http://dbpedia.org/resource/Snooker_world_rankings_1990/1991`
- Snooker world rankings 1991/1992: `http://dbpedia.org/resource/Snooker_world_rankings_1991/1992`

Example: `{"value": "1957-08-22", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1978", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (9 instances share this X).

- 1982__No._2:_Steve_Davis__1: `http://dbpedia.org/resource/Snooker_world_rankings_1981/1982__No._2:_Steve_Davis__1`
- Snooker world rankings 1983/1984: `http://dbpedia.org/resource/Snooker_world_rankings_1983/1984`
- Snooker world rankings 1987/1988: `http://dbpedia.org/resource/Snooker_world_rankings_1987/1988`

## 2. SnookerWorldRanking: birthDate → highestRank

Class: `http://dbpedia.org/ontology/SnookerWorldRanking`

X (literal): `http://dbpedia.org/ontology/birthDate`

Y (literal): `http://dbpedia.org/ontology/highestRank`

Scope: `whole_class`; excluded instances: 0; conflicting X groups: 0.

- X: 0 missing; 0 multivalued.
- Y: 0 missing; 0 multivalued.

Example: `{"value": "1969-01-13", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1", "datatype": "http://www.w3.org/2001/XMLSchema#integer", "language": null}` (15 instances share this X).

- 1990__No._3:_Stephen_Hendry__1: `http://dbpedia.org/resource/Snooker_world_rankings_1989/1990__No._3:_Stephen_Hendry__1`
- Snooker world rankings 1990/1991: `http://dbpedia.org/resource/Snooker_world_rankings_1990/1991`
- Snooker world rankings 1991/1992: `http://dbpedia.org/resource/Snooker_world_rankings_1991/1992`

Example: `{"value": "1957-08-22", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1", "datatype": "http://www.w3.org/2001/XMLSchema#integer", "language": null}` (9 instances share this X).

- 1982__No._2:_Steve_Davis__1: `http://dbpedia.org/resource/Snooker_world_rankings_1981/1982__No._2:_Steve_Davis__1`
- Snooker world rankings 1983/1984: `http://dbpedia.org/resource/Snooker_world_rankings_1983/1984`
- Snooker world rankings 1987/1988: `http://dbpedia.org/resource/Snooker_world_rankings_1987/1988`

## 3. Rocket: finalFlight → diameter

Class: `http://dbpedia.org/ontology/Rocket`

X (literal): `http://dbpedia.org/ontology/finalFlight`

Y (literal): `http://dbpedia.org/ontology/diameter`

Scope: `complete_single_valued_subset`; excluded instances: 256; conflicting X groups: 0.

- X: 133 missing; 30 multivalued.
- Y: 188 missing; 0 multivalued.

Example: `{"value": "1960-04-01", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "2.44", "datatype": "http://www.w3.org/2001/XMLSchema#double", "language": null}` (2 instances share this X).

- List of Thor-Able launches: `http://dbpedia.org/resource/List_of_Thor-Able_launches`
- Thor-Able: `http://dbpedia.org/resource/Thor-Able`

Example: `{"value": "1963-05-15", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "3.048", "datatype": "http://www.w3.org/2001/XMLSchema#double", "language": null}` (2 instances share this X).

- Atlas LV-3B: `http://dbpedia.org/resource/Atlas_LV-3B`
- List of Atlas LV3B launches: `http://dbpedia.org/resource/List_of_Atlas_LV3B_launches`

## 4. PopulatedPlace: areaCode → utcOffset

Class: `http://dbpedia.org/ontology/PopulatedPlace`

X (literal): `http://dbpedia.org/ontology/areaCode`

Y (literal): `http://dbpedia.org/ontology/utcOffset`

Scope: `complete_single_valued_subset`; excluded instances: 203; conflicting X groups: 0.

- X: 133 missing; 8 multivalued.
- Y: 67 missing; 86 multivalued.

Example: `{"value": "928", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "-7", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (164 instances share this X).

- Allah, Arizona: `http://dbpedia.org/resource/Allah,_Arizona`
- Allentown, Arizona: `http://dbpedia.org/resource/Allentown,_Arizona`
- Aripine, Arizona: `http://dbpedia.org/resource/Aripine,_Arizona`

Example: `{"value": "520", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "-7", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (156 instances share this X).

- Agua Caliente, Arizona: `http://dbpedia.org/resource/Agua_Caliente,_Arizona`
- Ak Komelik, Arizona: `http://dbpedia.org/resource/Ak_Komelik,_Arizona`
- Ali Ak Chin, Arizona: `http://dbpedia.org/resource/Ali_Ak_Chin,_Arizona`

## 5. RailwayStation: openingDate → openingYear

Class: `http://dbpedia.org/ontology/RailwayStation`

X (literal): `http://dbpedia.org/ontology/openingDate`

Y (literal): `http://dbpedia.org/ontology/openingYear`

Scope: `complete_single_valued_subset`; excluded instances: 1,378; conflicting X groups: 0.

- X: 1,368 missing; 10 multivalued.
- Y: 1,171 missing; 11 multivalued.

Example: `{"value": "2005-10-18", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "2005", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (22 instances share this X).

- Bangogae station: `http://dbpedia.org/resource/Bangogae_station`
- Beomeo station: `http://dbpedia.org/resource/Beomeo_station`
- Daegu Bank station: `http://dbpedia.org/resource/Daegu_Bank_station`

Example: `{"value": "1999-06-30", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1999", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (17 instances share this X).

- Buam station: `http://dbpedia.org/resource/Buam_station`
- Deokpo station: `http://dbpedia.org/resource/Deokpo_station`
- Dong-eui University station: `http://dbpedia.org/resource/Dong-eui_University_station`

## 6. Philosopher: birthDate → birthYear

Class: `http://dbpedia.org/ontology/Philosopher`

X (literal): `http://dbpedia.org/ontology/birthDate`

Y (literal): `http://dbpedia.org/ontology/birthYear`

Scope: `complete_single_valued_subset`; excluded instances: 1,053; conflicting X groups: 0.

- X: 1,053 missing; 0 multivalued.
- Y: 394 missing; 0 multivalued.

Example: `{"value": "1946-12-10", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1946", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (3 instances share this X).

- Alex Battler: `http://dbpedia.org/resource/Alex_Battler`
- Guy Hocquenghem: `http://dbpedia.org/resource/Guy_Hocquenghem`
- Raymond Geuss: `http://dbpedia.org/resource/Raymond_Geuss`

Example: `{"value": "1950-03-23", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1950", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (3 instances share this X).

- Charles Larmore: `http://dbpedia.org/resource/Charles_Larmore`
- Howard Lloyd Williams: `http://dbpedia.org/resource/Howard_Lloyd_Williams`
- Peter Simons (academic): `http://dbpedia.org/resource/Peter_Simons_(academic)`

## 7. Philosopher: deathDate → deathYear

Class: `http://dbpedia.org/ontology/Philosopher`

X (literal): `http://dbpedia.org/ontology/deathDate`

Y (literal): `http://dbpedia.org/ontology/deathYear`

Scope: `complete_single_valued_subset`; excluded instances: 1,671; conflicting X groups: 0.

- X: 1,671 missing; 0 multivalued.
- Y: 1,394 missing; 0 multivalued.

Example: `{"value": "1715-10-13", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1715", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (2 instances share this X).

- Frederik van Leenhof: `http://dbpedia.org/resource/Frederik_van_Leenhof`
- Nicolas Malebranche: `http://dbpedia.org/resource/Nicolas_Malebranche`

Example: `{"value": "1916-09-14", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1916", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (2 instances share this X).

- Josiah Royce: `http://dbpedia.org/resource/Josiah_Royce`
- Pierre Duhem: `http://dbpedia.org/resource/Pierre_Duhem`

## 8. Play: premiereDate → premiereYear

Class: `http://dbpedia.org/ontology/Play`

X (literal): `http://dbpedia.org/ontology/premiereDate`

Y (literal): `http://dbpedia.org/ontology/premiereYear`

Scope: `complete_single_valued_subset`; excluded instances: 2,149; conflicting X groups: 0.

- X: 2,124 missing; 16 multivalued.
- Y: 1,387 missing; 29 multivalued.

Example: `{"value": "1962-06-19", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1962", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (3 instances share this X).

- Plays for England: `http://dbpedia.org/resource/Plays_for_England`
- The Blood of the Bambergs: `http://dbpedia.org/resource/The_Blood_of_the_Bambergs`
- Under Plain Cover: `http://dbpedia.org/resource/Under_Plain_Cover`

Example: `{"value": "1899-10-23", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1899", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (2 instances share this X).

- Sherlock Holmes (play): `http://dbpedia.org/resource/Sherlock_Holmes_(play)`
- The Worst Woman in London: `http://dbpedia.org/resource/The_Worst_Woman_in_London`

## 9. Actor: birthDate → birthYear

Class: `http://dbpedia.org/ontology/Actor`

X (literal): `http://dbpedia.org/ontology/birthDate`

Y (literal): `http://dbpedia.org/ontology/birthYear`

Scope: `complete_single_valued_subset`; excluded instances: 1,828; conflicting X groups: 0.

- X: 1,433 missing; 0 multivalued.
- Y: 1,708 missing; 0 multivalued.

Example: `{"value": "1893-07-16", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1893", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (3 instances share this X).

- Abel Jacquin: `http://dbpedia.org/resource/Abel_Jacquin`
- Richard Cooper (actor): `http://dbpedia.org/resource/Richard_Cooper_(actor)`
- Toyo Fujita: `http://dbpedia.org/resource/Toyo_Fujita`

Example: `{"value": "1966-03-23", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1966", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (3 instances share this X).

- Beverly Hills (actress): `http://dbpedia.org/resource/Beverly_Hills_(actress)`
- Daniel Fathers: `http://dbpedia.org/resource/Daniel_Fathers`
- Itsumi Osawa: `http://dbpedia.org/resource/Itsumi_Osawa`

## 10. Actor: deathDate → deathYear

Class: `http://dbpedia.org/ontology/Actor`

X (literal): `http://dbpedia.org/ontology/deathDate`

Y (literal): `http://dbpedia.org/ontology/deathYear`

Scope: `complete_single_valued_subset`; excluded instances: 2,834; conflicting X groups: 0.

- X: 2,700 missing; 0 multivalued.
- Y: 2,774 missing; 0 multivalued.

Example: `{"value": "2014-11-22", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "2014", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (3 instances share this X).

- Derek Deadman: `http://dbpedia.org/resource/Derek_Deadman`
- Robin Langford: `http://dbpedia.org/resource/Robin_Langford`
- Stanley Lebor: `http://dbpedia.org/resource/Stanley_Lebor`

Example: `{"value": "1942-11-03", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1942", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (2 instances share this X).

- Eric Abrahamsson: `http://dbpedia.org/resource/Eric_Abrahamsson`
- Frederick Culley: `http://dbpedia.org/resource/Frederick_Culley`

## 11. PublicTransitSystem: openingDate → openingYear

Class: `http://dbpedia.org/ontology/PublicTransitSystem`

X (literal): `http://dbpedia.org/ontology/openingDate`

Y (literal): `http://dbpedia.org/ontology/openingYear`

Scope: `complete_single_valued_subset`; excluded instances: 3,105; conflicting X groups: 0.

- X: 3,045 missing; 48 multivalued.
- Y: 539 missing; 129 multivalued.

Example: `{"value": "1952-04-14", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1952", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (4 instances share this X).

- Eastern Railway zone: `http://dbpedia.org/resource/Eastern_Railway_zone`
- Northern Railway zone: `http://dbpedia.org/resource/Northern_Railway_zone`
- Railway in Haryana: `http://dbpedia.org/resource/Railway_in_Haryana`

Example: `{"value": "1846-07-16", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1846", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (3 instances share this X).

- Dunblane, Doune and Callander Railway: `http://dbpedia.org/resource/Dunblane,_Doune_and_Callander_Railway`
- London and North Western Railway: `http://dbpedia.org/resource/London_and_North_Western_Railway`
- Stirling and Dunfermline Railway: `http://dbpedia.org/resource/Stirling_and_Dunfermline_Railway`

## 12. Saint: deathDate → deathYear

Class: `http://dbpedia.org/ontology/Saint`

X (literal): `http://dbpedia.org/ontology/deathDate`

Y (literal): `http://dbpedia.org/ontology/deathYear`

Scope: `complete_single_valued_subset`; excluded instances: 2,658; conflicting X groups: 0.

- X: 2,656 missing; 1 multivalued.
- Y: 885 missing; 0 multivalued.

Example: `{"value": "1900-07-09", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1900", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (9 instances share this X).

- Amandina of Schakkebroek: `http://dbpedia.org/resource/Amandina_of_Schakkebroek`
- Francis Fogolla: `http://dbpedia.org/resource/Francis_Fogolla`
- Gregorio Grassi: `http://dbpedia.org/resource/Gregorio_Grassi`

Example: `{"value": "1535-05-04", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1535", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (6 instances share this X).

- Augustine Webster: `http://dbpedia.org/resource/Augustine_Webster`
- Carthusian Martyrs of London: `http://dbpedia.org/resource/Carthusian_Martyrs_of_London`
- John Haile: `http://dbpedia.org/resource/John_Haile`

## 13. Saint: birthDate → birthYear

Class: `http://dbpedia.org/ontology/Saint`

X (literal): `http://dbpedia.org/ontology/birthDate`

Y (literal): `http://dbpedia.org/ontology/birthYear`

Scope: `complete_single_valued_subset`; excluded instances: 3,617; conflicting X groups: 0.

- X: 3,616 missing; 0 multivalued.
- Y: 1,945 missing; 3 multivalued.

Example: `{"value": "1818-06-27", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1818", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (2 instances share this X).

- Bárbara Maix: `http://dbpedia.org/resource/Bárbara_Maix`
- James Lloyd Breck: `http://dbpedia.org/resource/James_Lloyd_Breck`

Example: `{"value": "1842-11-12", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1842", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (2 instances share this X).

- Franciszka Siedliska: `http://dbpedia.org/resource/Franciszka_Siedliska`
- Tomasa Ortiz Real: `http://dbpedia.org/resource/Tomasa_Ortiz_Real`

## 14. Bridge: openingDate → openingYear

Class: `http://dbpedia.org/ontology/Bridge`

X (literal): `http://dbpedia.org/ontology/openingDate`

Y (literal): `http://dbpedia.org/ontology/openingYear`

Scope: `complete_single_valued_subset`; excluded instances: 4,359; conflicting X groups: 0.

- X: 4,178 missing; 128 multivalued.
- Y: 1,987 missing; 297 multivalued.

Example: `{"value": "2011-10-19", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "2011", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (5 instances share this X).

- Gangai Bridge: `http://dbpedia.org/resource/Gangai_Bridge`
- Kayankerni Bridge: `http://dbpedia.org/resource/Kayankerni_Bridge`
- Ralkuli Bridge: `http://dbpedia.org/resource/Ralkuli_Bridge`

Example: `{"value": "2011-06-30", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "2011", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (4 instances share this X).

- Danyang–Kunshan Grand Bridge: `http://dbpedia.org/resource/Danyang–Kunshan_Grand_Bridge`
- Jiaozhou Bay Bridge: `http://dbpedia.org/resource/Jiaozhou_Bay_Bridge`
- Tianjin Grand Bridge: `http://dbpedia.org/resource/Tianjin_Grand_Bridge`

## 15. RailwayLine: openingDate → openingYear

Class: `http://dbpedia.org/ontology/RailwayLine`

X (literal): `http://dbpedia.org/ontology/openingDate`

Y (literal): `http://dbpedia.org/ontology/openingYear`

Scope: `complete_single_valued_subset`; excluded instances: 4,794; conflicting X groups: 0.

- X: 4,530 missing; 229 multivalued.
- Y: 2,564 missing; 359 multivalued.

Example: `{"value": "2017-12-28", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "2017", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (10 instances share this X).

- Huaibei–Xiaoxian railway: `http://dbpedia.org/resource/Huaibei–Xiaoxian_railway`
- Jiujiang–Quzhou railway: `http://dbpedia.org/resource/Jiujiang–Quzhou_railway`
- Line 10 (Chongqing Rail Transit): `http://dbpedia.org/resource/Line_10_(Chongqing_Rail_Transit)`

Example: `{"value": "2020-12-26", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "2020", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (10 instances share this X).

- Fuzhou–Pingtan railway: `http://dbpedia.org/resource/Fuzhou–Pingtan_railway`
- Huanghua–Dajiawa railway: `http://dbpedia.org/resource/Huanghua–Dajiawa_railway`
- Line 18 (Shanghai Metro): `http://dbpedia.org/resource/Line_18_(Shanghai_Metro)`

## 16. Drug: fdaUniiCode → chEMBL

Class: `http://dbpedia.org/ontology/Drug`

X (literal): `http://dbpedia.org/ontology/fdaUniiCode`

Y (literal): `http://dbpedia.org/ontology/chEMBL`

Scope: `complete_single_valued_subset`; excluded instances: 3,707; conflicting X groups: 0.

- X: 790 missing; 273 multivalued.
- Y: 3,310 missing; 80 multivalued.

Example: `{"value": "1JQS135EYN", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "395429", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (2 instances share this X).

- Oxytocin: `http://dbpedia.org/resource/Oxytocin`
- Oxytocin (medication): `http://dbpedia.org/resource/Oxytocin_(medication)`

Example: `{"value": "P6YC3EG204", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "2110563", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (2 instances share this X).

- Cyanocobalamin: `http://dbpedia.org/resource/Cyanocobalamin`
- Vitamin B12: `http://dbpedia.org/resource/Vitamin_B12`

## 17. Drug: casNumber → chEMBL

Class: `http://dbpedia.org/ontology/Drug`

X (literal): `http://dbpedia.org/ontology/casNumber`

Y (literal): `http://dbpedia.org/ontology/chEMBL`

Scope: `complete_single_valued_subset`; excluded instances: 3,787; conflicting X groups: 0.

- X: 385 missing; 653 multivalued.
- Y: 3,310 missing; 80 multivalued.

Example: `{"value": "11000-17-2", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "373742", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (2 instances share this X).

- Vasopressin: `http://dbpedia.org/resource/Vasopressin`
- Vasopressin (medication): `http://dbpedia.org/resource/Vasopressin_(medication)`

Example: `{"value": "50-56-6", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "395429", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (2 instances share this X).

- Oxytocin: `http://dbpedia.org/resource/Oxytocin`
- Oxytocin (medication): `http://dbpedia.org/resource/Oxytocin_(medication)`

## 18. Drug: fdaUniiCode → drugbank

Class: `http://dbpedia.org/ontology/Drug`

X (literal): `http://dbpedia.org/ontology/fdaUniiCode`

Y (literal): `http://dbpedia.org/ontology/drugbank`

Scope: `complete_single_valued_subset`; excluded instances: 5,244; conflicting X groups: 0.

- X: 790 missing; 273 multivalued.
- Y: 5,082 missing; 63 multivalued.

Example: `{"value": "1JQS135EYN", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "DB00107", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (2 instances share this X).

- Oxytocin: `http://dbpedia.org/resource/Oxytocin`
- Oxytocin (medication): `http://dbpedia.org/resource/Oxytocin_(medication)`

Example: `{"value": "3K9958V90M", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "DB00898", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (2 instances share this X).

- Alcohol (drug): `http://dbpedia.org/resource/Alcohol_(drug)`
- Alcohols (medicine): `http://dbpedia.org/resource/Alcohols_(medicine)`

## 19. Drug: fdaUniiCode → chEBI

Class: `http://dbpedia.org/ontology/Drug`

X (literal): `http://dbpedia.org/ontology/fdaUniiCode`

Y (literal): `http://dbpedia.org/ontology/chEBI`

Scope: `complete_single_valued_subset`; excluded instances: 5,672; conflicting X groups: 0.

- X: 790 missing; 273 multivalued.
- Y: 5,533 missing; 40 multivalued.

Example: `{"value": "1JQS135EYN", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "7872", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (2 instances share this X).

- Oxytocin: `http://dbpedia.org/resource/Oxytocin`
- Oxytocin (medication): `http://dbpedia.org/resource/Oxytocin_(medication)`

Example: `{"value": "X4W3ENH1CV", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "18357", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (2 instances share this X).

- Norepinephrine: `http://dbpedia.org/resource/Norepinephrine`
- Norepinephrine (medication): `http://dbpedia.org/resource/Norepinephrine_(medication)`

## 20. Drug: casNumber → chEBI

Class: `http://dbpedia.org/ontology/Drug`

X (literal): `http://dbpedia.org/ontology/casNumber`

Y (literal): `http://dbpedia.org/ontology/chEBI`

Scope: `complete_single_valued_subset`; excluded instances: 5,739; conflicting X groups: 0.

- X: 385 missing; 653 multivalued.
- Y: 5,533 missing; 40 multivalued.

Example: `{"value": "11000-17-2", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "9937", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (2 instances share this X).

- Vasopressin: `http://dbpedia.org/resource/Vasopressin`
- Vasopressin (medication): `http://dbpedia.org/resource/Vasopressin_(medication)`

Example: `{"value": "50-56-6", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "7872", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (2 instances share this X).

- Oxytocin: `http://dbpedia.org/resource/Oxytocin`
- Oxytocin (medication): `http://dbpedia.org/resource/Oxytocin_(medication)`

## 21. Drug: chEMBL → chEBI

Class: `http://dbpedia.org/ontology/Drug`

X (literal): `http://dbpedia.org/ontology/chEMBL`

Y (literal): `http://dbpedia.org/ontology/chEBI`

Scope: `complete_single_valued_subset`; excluded instances: 5,829; conflicting X groups: 0.

- X: 3,310 missing; 80 multivalued.
- Y: 5,533 missing; 40 multivalued.

Example: `{"value": "1437", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "18357", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (2 instances share this X).

- Norepinephrine: `http://dbpedia.org/resource/Norepinephrine`
- Norepinephrine (medication): `http://dbpedia.org/resource/Norepinephrine_(medication)`

Example: `{"value": "373742", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "9937", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` (2 instances share this X).

- Vasopressin: `http://dbpedia.org/resource/Vasopressin`
- Vasopressin (medication): `http://dbpedia.org/resource/Vasopressin_(medication)`

## 22. Dam: buildingStartDate → buildingStartYear

Class: `http://dbpedia.org/ontology/Dam`

X (literal): `http://dbpedia.org/ontology/buildingStartDate`

Y (literal): `http://dbpedia.org/ontology/buildingStartYear`

Scope: `complete_single_valued_subset`; excluded instances: 4,961; conflicting X groups: 0.

- X: 4,752 missing; 43 multivalued.
- Y: 4,832 missing; 17 multivalued.

Example: `{"value": "1972", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "1972", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (70 instances share this X).

- Agekura Dam: `http://dbpedia.org/resource/Agekura_Dam`
- Aguieira Dam: `http://dbpedia.org/resource/Aguieira_Dam`
- Akasaka Dam: `http://dbpedia.org/resource/Akasaka_Dam`

Example: `{"value": "1969", "datatype": "http://www.w3.org/2001/XMLSchema#string", "language": null}` → `{"value": "1969", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (58 instances share this X).

- Agigawa Dam: `http://dbpedia.org/resource/Agigawa_Dam`
- Andijan Dam: `http://dbpedia.org/resource/Andijan_Dam`
- Angostura Dam (Mexico): `http://dbpedia.org/resource/Angostura_Dam_(Mexico)`

## 23. GovernmentAgency: formationDate → formationYear

Class: `http://dbpedia.org/ontology/GovernmentAgency`

X (literal): `http://dbpedia.org/ontology/formationDate`

Y (literal): `http://dbpedia.org/ontology/formationYear`

Scope: `complete_single_valued_subset`; excluded instances: 5,692; conflicting X groups: 0.

- X: 5,531 missing; 119 multivalued.
- Y: 2,029 missing; 234 multivalued.

Example: `{"value": "1991-02-11", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1991", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (19 instances share this X).

- Ministry of Agriculture, Forestry and Water Economy (Serbia): `http://dbpedia.org/resource/Ministry_of_Agriculture,_Forestry_and_Water_Economy_(Serbia)`
- Ministry of Construction, Transport and Infrastructure (Serbia): `http://dbpedia.org/resource/Ministry_of_Construction,_Transport_and_Infrastructure_(Serbia)`
- Ministry of Construction and Urbanism (Serbia): `http://dbpedia.org/resource/Ministry_of_Construction_and_Urbanism_(Serbia)`

Example: `{"value": "1972-12-19", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1972", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (18 instances share this X).

- Department of Aboriginal Affairs: `http://dbpedia.org/resource/Department_of_Aboriginal_Affairs`
- Department of Education (1972–1983): `http://dbpedia.org/resource/Department_of_Education_(1972–1983)`
- Department of Environment and Conservation (Australia): `http://dbpedia.org/resource/Department_of_Environment_and_Conservation_(Australia)`

## 24. PoliticalParty: formationDate → formationYear

Class: `http://dbpedia.org/ontology/PoliticalParty`

X (literal): `http://dbpedia.org/ontology/formationDate`

Y (literal): `http://dbpedia.org/ontology/formationYear`

Scope: `complete_single_valued_subset`; excluded instances: 7,520; conflicting X groups: 0.

- X: 7,177 missing; 255 multivalued.
- Y: 1,742 missing; 468 multivalued.

Example: `{"value": "2018-03-02", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "2018", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (8 instances share this X).

- Alliance Party (Panama): `http://dbpedia.org/resource/Alliance_Party_(Panama)`
- Commoners'_Party_(Thailand)__Commoners'_Party__1: `http://dbpedia.org/resource/Commoners'_Party_(Thailand)__Commoners'_Party__1`
- New Alternative Party (Thailand): `http://dbpedia.org/resource/New_Alternative_Party_(Thailand)`

Example: `{"value": "2017-12-14", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "2017", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (6 instances share this X).

- Civic Area: `http://dbpedia.org/resource/Civic_Area`
- Community Sha Tin: `http://dbpedia.org/resource/Community_Sha_Tin`
- Lega (political party): `http://dbpedia.org/resource/Lega_(political_party)`

## 25. Station: openingDate → openingYear

Class: `http://dbpedia.org/ontology/Station`

X (literal): `http://dbpedia.org/ontology/openingDate`

Y (literal): `http://dbpedia.org/ontology/openingYear`

Scope: `complete_single_valued_subset`; excluded instances: 29,563; conflicting X groups: 0.

- X: 28,312 missing; 1,022 multivalued.
- Y: 19,054 missing; 1,420 multivalued.

Example: `{"value": "2013-12-28", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "2013", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (75 instances share this X).

- Baodaiqiao South station: `http://dbpedia.org/resource/Baodaiqiao_South_station`
- Baoji South railway station: `http://dbpedia.org/resource/Baoji_South_railway_station`
- Beijing Lu station: `http://dbpedia.org/resource/Beijing_Lu_station`

Example: `{"value": "2014-12-28", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "2014", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (68 instances share this X).

- Anli Lu station: `http://dbpedia.org/resource/Anli_Lu_station`
- Baizhuang station: `http://dbpedia.org/resource/Baizhuang_station`
- Baiziwan station: `http://dbpedia.org/resource/Baiziwan_station`

## 26. Person: birthDate → birthYear

Class: `http://dbpedia.org/ontology/Person`

X (literal): `http://dbpedia.org/ontology/birthDate`

Y (literal): `http://dbpedia.org/ontology/birthYear`

Scope: `complete_single_valued_subset`; excluded instances: 87,821; conflicting X groups: 0.

- X: 87,821 missing; 0 multivalued.
- Y: 48,220 missing; 1 multivalued.

Example: `{"value": "1970-01-01", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1970", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (21 instances share this X).

- Anne Dias-Griffin: `http://dbpedia.org/resource/Anne_Dias-Griffin`
- Claes Loberg: `http://dbpedia.org/resource/Claes_Loberg`
- Erik Bloodaxe (hacker): `http://dbpedia.org/resource/Erik_Bloodaxe_(hacker)`

Example: `{"value": "1941-01-01", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1941", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (20 instances share this X).

- Ali Khalifa Al-Kuwari: `http://dbpedia.org/resource/Ali_Khalifa_Al-Kuwari`
- Ali M. El-Agraa: `http://dbpedia.org/resource/Ali_M._El-Agraa`
- Amar Ezzahi: `http://dbpedia.org/resource/Amar_Ezzahi`

## 27. Person: deathDate → deathYear

Class: `http://dbpedia.org/ontology/Person`

X (literal): `http://dbpedia.org/ontology/deathDate`

Y (literal): `http://dbpedia.org/ontology/deathYear`

Scope: `complete_single_valued_subset`; excluded instances: 183,189; conflicting X groups: 0.

- X: 183,189 missing; 0 multivalued.
- Y: 170,284 missing; 0 multivalued.

Example: `{"value": "2001-09-11", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "2001", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (55 instances share this X).

- Abdulaziz al-Omari: `http://dbpedia.org/resource/Abdulaziz_al-Omari`
- Abraham Zelmanowitz: `http://dbpedia.org/resource/Abraham_Zelmanowitz`
- Ahmed al-Ghamdi: `http://dbpedia.org/resource/Ahmed_al-Ghamdi`

Example: `{"value": "1912-04-15", "datatype": "http://www.w3.org/2001/XMLSchema#date", "language": null}` → `{"value": "1912", "datatype": "http://www.w3.org/2001/XMLSchema#gYear", "language": null}` (37 instances share this X).

- Allison family: `http://dbpedia.org/resource/Allison_family`
- Allison_family__Bess_Allison__1: `http://dbpedia.org/resource/Allison_family__Bess_Allison__1`
- Allison_family__Loraine_Allison__1: `http://dbpedia.org/resource/Allison_family__Loraine_Allison__1`
