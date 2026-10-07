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
cores, memory and a wall-clock `runtime`; everything scheduler-specific lives in
`sites.yml`, managed by `custom_sites.py` (see
[Choose Where It Runs](#choose-where-it-runs)).

## Directory Structure

```
quickstart-workflow/
├── workflow_generator.py           # Pegasus workflow generator
├── custom_sites.py                 # Writes/merges sites.yml (also standalone)
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
./workflow_generator.py --container Quickstart_Container.sif
```

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

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--input-file` | `input/f.in` | Input file for the `hello` job (created if missing) |
| `--spin-time` | `3` | Seconds each job spins to simulate work |
| `--container` | (none) | Run jobs inside this Apptainer `.sif` image |
| `--submit` | false | Plan and submit the workflow, then wait and print statistics |
| `-e`, `--execution-site` | `compute` with a hosted catalog, else `local` | Site to plan against (`local` = the submit host); alias `--execution-site-name`. See [Choose Where It Runs](#choose-where-it-runs) for the other site options |
| `-s`, `--skip-sites-catalog` | false | Deprecated: same as `--site-style none` |
| `-o`, `--output` | `workflow.yml` | Output workflow file |

### Submit Workflow

```bash
pegasus-plan --submit -s local -o local workflow.yml
```

Note the line in the output starting with `pegasus-status` — it contains the
command to monitor the run, and the path to the submit directory holding all the
files needed to submit and monitor the workflow.

### Monitor Workflow

```bash
pegasus-status <run-directory>
pegasus-statistics <run-directory>
```

### Plan and Submit from Python

Because `workflow_generator.py` keeps a reference to the `Workflow` object, it
can plan, run, and monitor the workflow directly — these are wrappers around the
Pegasus CLI tools and accept the same arguments:

```bash
./workflow_generator.py --submit
```

which is equivalent to:

```python
workflow.wf.plan(sites=["local"], output_sites=["local"],
                 output_dir=workflow.local_storage_dir, submit=True)
workflow.wf.wait()          # block until the workflow finishes
workflow.wf.statistics()    # or workflow.wf.analyze() if it failed
```

### Run on an HTCondor Pool

The workflow above ran on the submit host because it was planned for the site
named `local`. To run the same abstract workflow on an HTCondor pool, replan it
for a different execution environment — no workflow changes are needed:

```bash
./workflow_generator.py -e condorpool --submit
```

On ACCESS Pegasus, `condorpool` jobs land on nodes provisioned from an ACCESS
resource such as Jetstream. Compare the hostname recorded in `output/f.out`
between the two runs to see where the jobs executed.

### Choose Where It Runs

The workflow never names a scheduler; `sites.yml` does. On every run
`workflow_generator.py` calls `custom_sites.ensure_sites_yml()`, which only fills
gaps, in this order of precedence:

1. **A `sites.yml` entry you provide** (by hand or with `custom_sites.py`) is
   kept as-is. Other sites in the file are never touched.
2. **A hosted catalog** named in `~/.pegasusrc`
   (`pegasus.catalog.site.repo.file`, e.g. on Unity) — Pegasus merges the local
   `sites.yml` over it. Hosted catalogs define one site, `compute`, which then
   becomes the default `-e`.
3. **A default HTCondor site** is added for any other `-e` (e.g. `condorpool`).

A `local` site (`./scratch`, `./output`) is always ensured, since `-o local` and
`-e local` need it. The generated `pegasus.properties` names `sites.yml`
explicitly, so `pegasus-plan` finds it from any directory.

```bash
# Submit host (default without a hosted catalog)
./workflow_generator.py

# HTCondor pool
./workflow_generator.py -e condorpool

# Cluster with a hosted catalog: add your account to its "compute" site
./workflow_generator.py --site-style slurm --project my_lab

# Slurm cluster without a hosted catalog
./workflow_generator.py -e compute --site-style slurm --queue cpu \
    --project my_lab --site-scratch /scratch/$USER/quickstart
```

Against a hosted catalog, `--site-style slurm` writes only your overrides
(queue, project, profiles) for `compute` as an overlay, not the whole site; a
style that contradicts the hosted entry is rejected. A site the hosted catalog
does not define (e.g. `-e condorpool --site-style condor` next to a hosted
`compute`) gets a complete entry instead; without `--site-style`, the generator
warns that planning against it will fail.

| Option | Default | Meaning |
|---|---|---|
| `-e, --execution-site` | `compute` with a hosted catalog, else `local` | Site to plan against. |
| `--site-style` | `auto` | `auto`: keep what exists, else add an HTCondor site. `condor`/`slurm`: (re)write this site's entry. `none`: don't touch `sites.yml`. |
| `--queue`, `--project` | — | Partition and account on a batch site (`pegasus.queue`, `pegasus.project`). `--queue` is required for a Slurm site without a hosted catalog. |
| `--site-scratch` | `./work` | Slurm: shared scratch visible to the workers and the submit host. |
| `--site-profile` | — | Extra `NS:KEY=VALUE` profile on the site, e.g. `pegasus:glite.arguments=--constraint=avx512`; repeatable. |
| `--shared-filesystem` | `auto` | `auto`: jobs read inputs straight from the submit host on Slurm sites (`pegasus.transfer.bypass.input.staging`), never on HTCondor. `yes`/`no` force it. |
| `--sites-yml` | `sites.yml` | Local site catalog to manage. |

Each job carries a `runtime` of 600 s (`TOOL_CONFIGS`), which batch sites
require and enforce; condor pools ignore it. With `--container` on a batch site,
the workflow directory is bound into the container so inputs staged as symlinks
(`pegasus.transfer.links`) resolve inside it.

`custom_sites.py` also runs standalone, e.g. to prepare `sites.yml` once:

```bash
./custom_sites.py --style slurm --project my_lab            # overlay on a hosted catalog
./custom_sites.py --style condor --site condorpool --full   # HTCondor pool
```

## Outputs

| Output | Description |
|--------|-------------|
| `output/f.out` | Final output: execution hostnames plus the accumulated input contents |

`f.inter` is an intermediate file (`stage_out=False`), so it stays in scratch and
is not copied to `output/`.

```bash
cat output/f.out
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
