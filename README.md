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
the DAG, which is why the same workflow can run on the submit host and on an
HTCondor pool without being redefined.

## Directory Structure

```
quickstart-workflow/
├── workflow_generator.py           # Pegasus workflow generator
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
| `-e`, `--execution-site-name` | `local` | Execution site name (`local` = the submit host) |
| `-s`, `--skip-sites-catalog` | false | Skip site catalog creation |
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

## Running on FABRIC

The workflow can also be run on the [FABRIC testbed](https://fabric-testbed.net/)
by deploying a distributed Pegasus/HTCondor cluster across FABRIC sites.

### Deploy a Pegasus/HTCondor Cluster

You can provision a cluster using either of the following notebooks:

| Option | Link | Description |
|--------|------|-------------|
| FABRIC Artifact (Recommended) | [Pegasus-FABRIC Artifact](https://artifacts.fabric-testbed.net/artifacts/53da4088-a175-4f0c-9e25-a4a371032a39) | Pre-configured notebook from the FABRIC Artifacts repository |
| Jupyter Examples | [pegasus-fabric.ipynb](https://github.com/fabric-testbed/jupyter-examples/blob/f7be0c75f22544c72d7b3e3fa42bbdfd9d8bb841/fabric_examples/complex_recipes/pegasus/pegasus-fabric.ipynb) | Notebook from the official FABRIC Jupyter examples |

Both notebooks provision the following cluster architecture:

- **Submit Node** -- Central Manager running HTCondor scheduler and Pegasus WMS
- **Worker Nodes** -- Distributed execution points across multiple FABRIC sites
- **FABNetv4 Networking** -- Private L3 network connecting all nodes

### Setup Steps

1. Log into the [FABRIC JupyterHub](https://jupyter.fabric-testbed.net/)
2. Upload or clone one of the Pegasus-FABRIC notebooks above
3. Configure your desired sites and node specifications
4. Run the notebook to provision the cluster
5. Clone this repository on the submit node
6. Generate and submit the workflow on the submit node

## Dependencies

- Python 3.8+ (standard library only)
- `psutil` (optional) — used by `pegasus-keg.py` to spin the CPU for
  `--spin-time` seconds; without it the script sleeps instead
