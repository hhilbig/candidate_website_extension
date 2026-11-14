# U.S. House and Senate candidate websites

This repository contains the code for the dataset "U.S. House (2018–2026) and Senate (2002–2026) Candidate Websites" (Hanno Hilbig). The dataset holds the text of campaign websites of Democratic and Republican candidates. It extends the House corpus of Di Tella, Kotti, Le Pennec, and Pons (2025; ICPSR 226001) through 2026 and adds Senate candidates from 2002.

The data are on Harvard Dataverse: https://doi.org/10.7910/DVN/BZ2JRS (version 5.0, November 2026). Coverage and validation results are at https://www.hannohilbig.com/candidatewebsites/.

Version 5.0 covers 18,697 candidate-years from FEC records. For 9,074 of them the data hold website text, 896,487 pages in total. Pages for 2002–2024 come from the Internet Archive. For 2026, they come from the Internet Archive and from copies the project made of the campaign websites between September 14 and November 5, 2026. Six further files hold issue statements, biography facts, and endorsements extracted from the pages with language models.

Download the data from Dataverse. This repository does not include the data or the intermediate files from collection, so most scripts will not run on their own.

## Contents

- `docs/RELEASE_README.md`, `docs/codebook.md`, `docs/candidate_extraction_codebook.md`: the documentation published with version 5.0 (population, files, variables, limitations).
- `docs/collection_2026.md`: how the 2026 websites were collected.
- `src/`: roster building, Wayback collection, text extraction, and the 2026 collection.
- `scripts/`: release builds (`rebuild_release.py`, `assemble_release_v5_0.py`), validation (`validate_release.py`), and the extraction and validation steps.
- `scripts/hive/`: language-model jobs run on a university GPU cluster.
- `config/`: build inputs and reviewed adjudications.
- `tests/`: unit tests. Tests that read the collected data are not included.

## Setup

```
python3 -m venv .venv
.venv/bin/pip install -r requirements-release.txt
.venv/bin/python -m pytest -q
```

The collection scripts call the OpenFEC API. Copy `.env.example` to `.env` and add a free key from https://api.open.fec.gov/signup/.

## Terms of use

The compilation, derived variables, and documentation are released under CC BY 4.0. Copyright in the campaign material remains with its authors. To request removal of material you hold rights to, use the contact form on the Dataverse record.

Please cite the dataset as shown on its Dataverse page.
