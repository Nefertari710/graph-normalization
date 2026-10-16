# Theory Experiments

This directory supports the paper's schema-normalization evaluation (RQ1): whether a schema can reach Knowledge Graph Normal Form (KGNF), which functional dependencies (FDs) remain blocked, and how normalization reduces logical redundancy.

| Script | What it evaluates | Saved results |
| --- | --- | --- |
| [check_kgnf_running_example.py](scripts/check_kgnf_running_example.py) | Whether the fixed `OrderInfo` example can reach KGNF under two FD sets. | [Running-example results](report/check_kgnf_running_example.md) |
| [normalize_schemas.py](scripts/normalize_schemas.py) | Normalize on 256 TPC-H schema variants, Offshore property FDs, and saved DBpedia FDs. | [Schema-normalization results](report/normalize_schemas.md) |
| [reference_fd_experiment.py](scripts/reference_fd_experiment.py) | Predefined FDs over properties and references in Northwind and Offshore: decomposition results and logical storage reduction. | [Reference-FD results](report/reference_fd_experiment.md) |

## Run

Use Python 3.13 and run commands from the **repository root**. **Neo4j and GDS are not required.** The Reference-FD script needs `pandas`, included in [requirements.txt](../requirements.txt).

The first two checks use inputs already included in the repository, including the [saved DBpedia FDs](../extend_experiment/dbpedia_experiment/find_functional_dependency/functional_dependencies_all.json):

```bash
python theory/scripts/check_kgnf_running_example.py
python theory/scripts/normalize_schemas.py
```

The Reference-FD experiment reads Northwind and Offshore CSVs. Northwind is included; download Offshore before running:

```bash
python offshore_experiment/dataset/download_data.py
python theory/scripts/reference_fd_experiment.py
```

## Results

The running-example script prints to the terminal; its saved results above are provided for reference. The other two scripts update their reports in `theory/report/` and use 20 randomized decomposition orders per workload by default.

Reported times measure schema normalization. Storage estimates count logical feature occurrences. Details of the measurements are in the linked reports.
