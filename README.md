# Candidate Website Extension

This repository builds a dataset of archived US congressional campaign
websites. It extends the House candidate corpus assembled by Di Tella, Kotti,
Le Pennec, and Pons (2025) from 2018 through 2024 and adds Senate candidates
from 2002 through 2024. The release contains 7,353 captured candidate-years
drawn from 799,058 archived pages.

The candidate population consists of Democratic and Republican House and
Senate candidates whose FEC-recorded election year equals the target year,
plus reviewed general-election ballot exceptions. The collection did not
re-scrape the House years already covered by ICPSR 226001.

## Release documentation

- [Release README](docs/RELEASE_README.md): population, five-file inventory,
  construction, limitations, terms of use, and reproduction commands
- [Codebook](docs/deliverable_codebook.md): fields, keys, and missing values
- [Website](https://www.hannohilbig.com/candidatewebsites/): coverage and
  validation results

The repository supports the release but is not part of the openICPSR deposit.

## Rebuild and validate the release

The release build uses CPython 3.14.3 and the locked packages in
`requirements-release.txt`. Source archives and public source files must be at
the paths documented in the build configuration.

```bash
python3 -m venv .venv-release
.venv-release/bin/python -m pip install -r requirements-release.txt
.venv-release/bin/python scripts/rebuild_release.py --out-dir build/release_candidate
.venv-release/bin/python scripts/validate_release.py build/release_candidate
```

The build writes the five data products and `manifest.json` to a separate
staging directory. It does not overwrite an existing release.

## Run the collection pipeline

The collection commands use public data services and may make live network
requests. Install their dependencies with:

```bash
pip install -r requirements.txt
```

Build a candidate roster:

```bash
python -m src.build_candidate_roster --office house --year 2022
python -m src.build_candidate_roster --office senate --years 2002-2024
```

Retrieve archived sites from the Wayback Machine:

```bash
python -m src.scrape_wayback --office house --year 2022
python -m src.scrape_wayback --roster data/rosters/roster_senate_2020.csv
```

`config/config.yaml` controls office-years, rate limits, retry behavior, crawl
depth, URL sources, and output paths. [SPEC.md](SPEC.md) documents the
collection schema and rules.

## Citation

Please cite this release and the original dataset that it extends:

> Hilbig, Hanno. 2026. “U.S. Congressional Candidate Websites, 2002–2024.”
> Version 1.0.0. openICPSR.

> Di Tella, Rafael, Randy Kotti, Caroline Le Pennec, and Vincent Pons. 2025.
> “Keep Your Enemies Closer: Strategic Platform Adjustments during U.S. and
> French Elections.” openICPSR 226001. <https://doi.org/10.3886/E226001V1>
