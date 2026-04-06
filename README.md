<!--
SPDX-FileCopyrightText:  PyPSA-Earth and PyPSA-Eur Authors

SPDX-License-Identifier: AGPL-3.0-or-later
-->

# PyPSA-AUS-efuels
Repository for the PyPSA-AUS-efuels project

<img src="https://raw.githubusercontent.com/open-energy-transition/oet-website/main/assets/img/oet-logo-red-n-subtitle.png" alt="Open Energy Transition Logo" width="260" height="100" align="right">

This repository is maintained using [OET's soft-fork strategy](https://open-energy-transition.github.io/handbook/docs/Engineering/SoftForkStrategy). OET's primary aim is to contribute as much as possible to the open source (OS) upstream repositories. For long-term changes that cannot be directly merged upstream, the strategy organizes and maintains OET forks, ensuring they remain up-to-date and compatible with upstream, while also supporting future contributions back to the OS repositories.

## Installation
More details on configuration, installation, and debugging is available at [OET's soft-fork](https://github.com/open-energy-transition/pypsa-earth).

## Running the model
- Run a dryrun of the Snakemake workflow by copying the following command:
  ```bash
  snakemake -c1 solve_sector_networks -n
  ```
- Run the Snakemake workflow by copying the following command:
  ```bash
  snakemake -c1 solve_all_networks
  ```

## Repository structure and workflow

This repository contains the *PyPSA-AUS-efuels* project and uses OET’s soft fork of PyPSA-Earth as a submodule.

### PyPSA-Earth submodule

The `pypsa-earth/` directory is a Git submodule pointing to:

https://github.com/open-energy-transition/pypsa-earth

This project follows OET’s soft fork strategy:

- open-energy-transition/pypsa-earth:main  
  contains the OET soft fork, kept aligned with upstream

- open-energy-transition/pypsa-earth:project-aus-efuel  
  contains project-specific modifications required for this project

This repository depends on the `project-aus-efuel` branch of the soft fork.

---

### Cloning the repository

To clone the repository including the submodule:

```bash
git clone --recurse-submodules git@github.com:open-energy-transition/pypsa-aus-efuels.git
cd pypsa-aus-efuels
```

If the repository was cloned without submodules:

```bash
git submodule update --init --recursive
```

---

## Updating PyPSA-Earth

Updates to PyPSA-Earth are handled explicitly to remain consistent with the OET's soft fork strategy.

### Step 1 — Synchronize the project branch with main

```bash
cd pypsa-earth
git fetch origin
git checkout project-aus-efuel
git merge origin/main
```

Resolve any conflicts if necessary, then push:

```bash
git push origin project-aus-efuel
```

---

### Step 2 — Update the submodule reference

After updating the project branch, update the submodule pointer in this repository:

```bash
cd ..
git add pypsa-earth
git commit -m "Update pypsa-earth submodule"
git push
```

---

## Important notes

- The submodule points to a specific commit, not automatically to the latest version of a branch.
- Updating PyPSA-Earth always requires two steps:
  1. updating the project branch in the soft fork
  2. updating the submodule reference in this repository
- Merge conflicts must be resolved manually. This process is intentionally not automated.

---

## Development guidelines

- Changes to the PyPSA-Earth workflow should be made in  
  open-energy-transition/pypsa-earth, on the project-aus-efuel branch.
- Project-specific configuration, scripts, and analysis belong in this repository.

**More details will be added during the project execution.**

----

<sup>*</sup> Open Energy Transition (g)GmbH, Königsallee 52, 95448 Bayreuth, Germany

----
