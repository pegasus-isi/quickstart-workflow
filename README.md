# Quickstart (Hello World) Pegasus Workflow

A [Pegasus WMS](https://pegasus.isi.edu/) workflow that generates, plans, and
executes a two-job Hello World pipeline. It is the smallest useful workflow in
this collection — use it to learn the Pegasus API, or to validate a fresh
Pegasus/HTCondor installation before running a real pipeline.

## Pipeline Overview

Rectangles represent input/output files, ovals represent compute jobs, and the
arrows are file dependencies. The **world** job depends on the output of
**hello**.

```
f.in ──> hello ──> f.inter ──> world ──> f.out
```

![Hello World Workflow](./images/pipeline.svg)

| Step | Tool | Description |
|------|------|-------------|
| 1. hello | `pegasus-keg.py` | Reads `f.in`, records the execution host, writes `f.inter` |
| 2. world | `pegasus-keg.py` | Reads `f.inter`, records the execution host, writes `f.out` |

Both jobs invoke the same script: `bin/hello.py` and `bin/world.py` are symlinks
to `bin/pegasus-keg.py`, so each job name matches its executable name and is
easy to spot in the logs. The script reads its input file, captures the hostname
of the node it ran on, and echoes both into its output — so `f.out` shows where
each job executed.

The abstract workflow description is portable: it contains no physical file
locations, executable paths, or cluster endpoints. Those come from the Replica,
Transformation, and Site catalogs that `workflow_generator.py` writes alongside
the DAG, which is why the same workflow can run on the submit host, on an
HTCondor pool, or on a Slurm cluster without being redefined. Jobs state only
cores and memory; where they run comes from a site catalog you choose, never
from the generator (see [Choose Where It Runs](#choose-where-it-runs)).

## Directory Structure

```
quickstart-workflow/
├── workflow_generator.py           # Pegasus workflow generator (writes catalogs, never submits)
├── Quickstart-Workflow.ipynb       # Notebook driving the same generator class
├── bin/
│   ├── pegasus-keg.py              # The one tool this workflow runs
│   ├── hello.py -> pegasus-keg.py  # Symlink so the job name matches the executable
│   └── world.py -> pegasus-keg.py
├── Apptainer/
│   └── Quickstart_Container.def    # Optional container (python3 + psutil)
├── input/
│   └── f.in                        # Sample input file
├── images/
│   └── pipeline.svg
├── run_manual.sh                   # Run each step locally, without Pegasus
└── README.md
```

## Prerequisites

- [Pegasus WMS](https://pegasus.isi.edu/) >= 5.0
- [HTCondor](https://htcondor.org/) >= 10.2
- Python 3.8+
- [Apptainer](https://apptainer.org/) (optional — only for containerized execution)

## Setup

### 1. Prepare Input Data

`input/f.in` is included in the repository. If it is missing,
`workflow_generator.py` recreates it with sample contents on the next run. Pass
`--input-file PATH` to use your own file instead.

### 2. Build the Apptainer Container (optional)

The workflow runs on the bare Python interpreter by default. To run the jobs
inside a container instead:

```bash
apptainer build Quickstart_Container.sif Apptainer/Quickstart_Container.def
./workflow_generator.py --container Quickstart_Container.sif -e condorpool
```

The image is Debian 13 (`python:3.11-slim-trixie`) and has no curl/wget, so it
cannot download a Pegasus worker package for itself (the job dies with exit 71,
*Unable to find curl/wget*). With `--container`, the generator asks
`pegasus-version` for the planner's version and stages the matching
`x86_64_deb_13` worker package into each job (`pegasus::worker` in the
transformation catalog, `pegasus.transfer.worker.package.autodownload = false`).
If you change the base image, change `WORKER_PACKAGE_PLATFORM` in
`workflow_generator.py` to match.

## Usage

### Test Locally First

```bash
./run_manual.sh
```

This runs both steps outside Pegasus and prints the final output, confirming the
scripts and arguments line up before anything is submitted.

### Generate Workflow

```bash
./workflow_generator.py --output workflow.yml
```

The generator writes `workflow.yml` and its catalogs, then prints the
`pegasus-plan` command. It never plans or submits by itself.

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--input-file` | `input/f.in` | Input file for the `hello` job (created if missing) |
| `--spin-time` | `3` | Seconds each job spins to simulate work |
| `--container` | (none) | Run jobs inside this Apptainer `.sif` image |
| `-s`, `--hosted-site-catalog` | (none; `~/.pegasusrc` if set) | [Hosted site catalog](https://github.com/pegasushub/pegasus-site-catalogs/tree/main/conf) to plan against, e.g. `access-pegasus.yml`, `unity.yml`; written to `pegasus.properties` |
| `-e`, `--execution-site-name` | `compute` | Execution site name; `condorpool` on a plain HTCondor pool with no site catalog, `local` for the submit host |
| `-o`, `--output` | `workflow.yml` | Output workflow file |

### Plan and Submit

```bash
pegasus-plan --dir submit -s compute -o local --submit workflow.yml
```

Use the `-e` value you generated with as `-s` here. Note the line in the output
starting with `pegasus-status` — it contains the command to monitor the run, and
the path to the submit directory holding all the files needed to submit and
monitor the workflow.

### Monitor Workflow

```bash
pegasus-status <run-directory>
pegasus-statistics <run-directory>
```

### The Notebook

`Quickstart-Workflow.ipynb` runs the same steps interactively. It imports
`QuickstartWorkflow` from `workflow_generator.py` and calls its methods, so the
pipeline is defined in one place only, then plans and submits from an explicit
cell (`workflow.plan_submit()`), monitors with `status()`/`wait()` and reports
with `statistics()`.

### Choose Where It Runs

Where jobs run depends on your resource provider and allocation, so it lives in
a site catalog you choose, not in the workflow. Jobs run on a site named
`compute`, the one site every centrally hosted catalog
([pegasus-site-catalogs](https://github.com/pegasushub/pegasus-site-catalogs/tree/main/conf))
defines; `pegasus-plan` downloads the catalog from the branch matching its
Pegasus version.

```bash
# A hosted catalog, named per workflow
./workflow_generator.py -s access-pegasus.yml

# ...or once per user, in ~/.pegasusrc (as the ACCESS training setup does):
#   pegasus.catalog.site.repo.file = unity.yml
#   env.RESOURCE_USERNAME = jdoe
#   env.RESOURCE_PROJECT = my_lab
./workflow_generator.py

# A plain HTCondor pool with no site catalog: Pegasus provides "condorpool"
./workflow_generator.py -e condorpool
pegasus-plan --dir submit -s condorpool -o local --submit workflow.yml

# The submit host
./workflow_generator.py -e local
pegasus-plan --dir submit -s local -o local --submit workflow.yml
```

Pegasus has no built-in `compute` site: with no hosted catalog configured,
`-e compute` fails to plan with *"Execution site compute not loaded into site
store"*. Use `-e condorpool` there, or the notebook, which writes a local
HTCondor `compute` site with `create_sites_catalog()`.

Compare the hostname recorded in `f.out` between runs on different sites to
see where the jobs executed.

## Outputs

| Output | Description |
|--------|-------------|
| `f.out` | Final output: execution hostnames plus the accumulated input contents |

Where it lands depends on the `local` site in use. The CLI writes no site
catalog, so Pegasus uses its default local storage, `./wf-output/`; the
notebook's `create_sites_catalog()` sets it to `./output/`.

`f.inter` is an intermediate file (`stage_out=False`), so it stays in scratch.

```bash
cat wf-output/f.out     # CLI run
cat output/f.out        # notebook run
```

## Resource Requirements

| Step | Memory | Cores |
|------|--------|-------|
| hello | 1 GB | 1 |
| world | 1 GB | 1 |

## Dependencies

- Python 3.8+ (standard library only)
- `psutil` (optional) — used by `pegasus-keg.py` to spin the CPU for
  `--spin-time` seconds; without it the script sleeps instead
