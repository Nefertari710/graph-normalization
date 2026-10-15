# Provider and Service

Compare redundancy, update times, and graph embeddings before and after provider/service normalization.

Complete the [shared setup](../../README.md#setup). Run all commands from the repository root.

## 1. Download and import

The [CMS 2024 CSV](https://data.cms.gov/sites/default/files/2026-05/b5ebab5a-f490-418a-9bce-4b9f31419356/PHY_R26_P05_V10_D24_Prov_Svc.csv), released in May 2026, is approximately **3.25 GB**. The downloader saves it to `dataset/data/PHY_R26_P05_V10_D24_Prov_Svc.csv` within this directory.

Use Neo4j Enterprise and an empty target graph. The default database is `providerandservicecsv`.

```bash
python extend_experiment/providerAndService_experiment/dataset/download_data.py
python extend_experiment/providerAndService_experiment/dataset/import_database.py
```

Imported schema: each CSV row becomes a `ProviderServiceRecord` node; the initial graph has no relationships.

## 2. Run experiments and reports

Redundancy and updates:

```bash
python extend_experiment/providerAndService_experiment/redundancy/scripts/redundancy_experiment.py
python extend_experiment/providerAndService_experiment/redundancy/analysis/analysis.py
```

Outputs: JSON and Markdown in [`redundancy/results/providerandservicecsv/`](redundancy/results/providerandservicecsv/).

Embeddings (**GDS required**):

```bash
python extend_experiment/providerAndService_experiment/embedding/scripts/embedding_provideandservice.py
python extend_experiment/providerAndService_experiment/embedding/analysis/analysis.py
```

Outputs: [`embedding/results/embedding_providerandservice.json`](embedding/results/embedding_providerandservice.json) and [`embedding/analysis/result.md`](embedding/analysis/result.md).

Historical source hashes were not recorded, so equality with the original experiment data is unconfirmed.
