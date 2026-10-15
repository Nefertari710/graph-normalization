# Hospital

Compare redundancy and update times using [CMS hospital care measures](https://data.cms.gov/provider-data/dataset/yv7e-xc69).

Complete the [setup](../../README.md#setup). Run all commands from the repository root.

## 1. Import data

The [CSV](dataset/Timely_and_Effective_Care-Hospital.csv) is included in `dataset/`. Import requires Enterprise and an empty database; the default is `hospitalcsv`.

```bash
python extend_experiment/hospital_experiment/dataset/import_database.py
```

## 2. Run the experiment and analysis

```bash
python extend_experiment/hospital_experiment/redundancy/scripts/redundancy_experiment.py
python extend_experiment/hospital_experiment/redundancy/analysis/analysis.py
```

Outputs: `redundancy_experiment.json` and `.md` in [redundancy/results/hospitalcsv/](redundancy/results/hospitalcsv/). GDS is not required.
