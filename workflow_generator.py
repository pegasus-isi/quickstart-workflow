#!/usr/bin/env python3

"""
Pegasus workflow generator for the quickstart (Hello World) workflow.

A minimal two-job pipeline used to learn the Pegasus API and to validate a
Pegasus/HTCondor installation. Each job runs `pegasus-keg.py`, which reads its
input file, records the hostname of the node it ran on, and writes both to its
output file — so the final output shows where each job executed.

Pipeline steps:
1. hello - reads f.in, records the execution host, writes f.inter
2. world - reads f.inter, records the execution host, writes f.out

Usage:
    ./workflow_generator.py
    ./workflow_generator.py -e condorpool -o workflow.yml
    ./workflow_generator.py --submit          # plan, submit, wait, statistics
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from Pegasus.api import *

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Per-tool resource configuration. Both jobs run the same tiny script, so the
# requirements are identical — kept as a table for consistency with the other
# workflows in this collection.
TOOL_CONFIGS = {
    "hello": {"memory": "1 GB", "cores": 1},
    "world": {"memory": "1 GB", "cores": 1},
}

DEFAULT_INPUT_CONTENTS = (
    "This is the contents of the input file for the hello world workflow!"
)


class QuickstartWorkflow:
    """Two-job Hello World workflow: hello -> world."""

    wf = None
    sc = None
    tc = None
    rc = None
    props = None

    dagfile = None
    wf_dir = None
    shared_scratch_dir = None
    local_storage_dir = None
    wf_name = "hello-world"

    def __init__(self, dagfile="workflow.yml", input_file=None, container_image=None):
        self.dagfile = dagfile
        self.wf_dir = str(Path(__file__).parent.resolve())
        self.shared_scratch_dir = os.path.join(self.wf_dir, "scratch")
        self.local_storage_dir = os.path.join(self.wf_dir, "output")
        self.input_file = input_file or os.path.join(self.wf_dir, "input", "f.in")
        self.container_image = container_image

    def write(self):
        """Write all catalogs and the workflow to files."""
        if self.sc is not None:
            self.sc.write()
        self.props.write()
        self.rc.write()
        self.tc.write()
        self.wf.write(file=self.dagfile)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------
    def create_pegasus_properties(self):
        self.props = Properties()
        self.props["pegasus.transfer.threads"] = "16"

    # ------------------------------------------------------------------
    # Site Catalog
    # ------------------------------------------------------------------
    def create_sites_catalog(self, exec_site_name="local"):
        self.sc = SiteCatalog()

        local = Site("local").add_directories(
            Directory(
                Directory.SHARED_SCRATCH, self.shared_scratch_dir
            ).add_file_servers(
                FileServer("file://" + self.shared_scratch_dir, Operation.ALL)
            ),
            Directory(
                Directory.LOCAL_STORAGE, self.local_storage_dir
            ).add_file_servers(
                FileServer("file://" + self.local_storage_dir, Operation.ALL)
            ),
        )
        self.sc.add_sites(local)

        # When jobs run on the submit host itself, "local" is the execution
        # site too and must not be declared twice.
        if exec_site_name != "local":
            exec_site = (
                Site(exec_site_name)
                .add_condor_profile(universe="vanilla")
                .add_pegasus_profile(style="condor")
            )
            self.sc.add_sites(exec_site)

    # ------------------------------------------------------------------
    # Transformation Catalog
    # ------------------------------------------------------------------
    def create_transformation_catalog(self, exec_site_name="local"):
        self.tc = TransformationCatalog()

        # The quickstart runs on the bare Python interpreter by default. Pass
        # --container <path>.sif to run the jobs inside the Apptainer image
        # built from Apptainer/Quickstart_Container.def instead.
        container = None
        if self.container_image:
            container = Container(
                "quickstart_container",
                container_type=Container.SINGULARITY,
                image="file://" + os.path.abspath(self.container_image),
                image_site="local",
            )
            self.tc.add_containers(container)

        # bin/hello.py and bin/world.py are symlinks to the same script,
        # bin/pegasus-keg.py — the job name and the executable name match, which
        # makes the job easy to spot in the logs.
        transformations = []
        for tool_name, config in TOOL_CONFIGS.items():
            tx = Transformation(
                tool_name,
                site=exec_site_name,
                pfn=os.path.join(self.wf_dir, f"bin/{tool_name}.py"),
                is_stageable=True,
                container=container,
            ).add_pegasus_profile(memory=config["memory"], cores=config["cores"])
            transformations.append(tx)

        self.tc.add_transformations(*transformations)

    # ------------------------------------------------------------------
    # Replica Catalog
    # ------------------------------------------------------------------
    def create_replica_catalog(self):
        self.rc = ReplicaCatalog()
        self.rc.add_replica(
            "local", "f.in", "file://" + os.path.abspath(self.input_file)
        )

    # ------------------------------------------------------------------
    # Workflow DAG
    # ------------------------------------------------------------------
    def create_workflow(self, args):
        """Build the two-job DAG: hello -> world."""
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        fin = File("f.in")
        finter = File("f.inter")
        fout = File("f.out")

        job_hello = (
            Job("hello", _id="hello", node_label="hello")
            .add_args("-T", str(args.spin_time), "-i", fin, "-o", finter)
            .add_inputs(fin)
            .add_outputs(finter, stage_out=False, register_replica=False)
        )

        # world consumes the same File object hello produced, so Pegasus infers
        # the dependency — no explicit add_dependency() needed.
        job_world = (
            Job("world", _id="world", node_label="world")
            .add_args("-T", str(args.spin_time), "-i", finter, "-o", fout)
            .add_inputs(finter)
            .add_outputs(fout, stage_out=True, register_replica=False)
        )

        self.wf.add_jobs(job_hello, job_world)


# ======================================================================
# main() — CLI argument parsing
# ======================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Generate the Pegasus quickstart (Hello World) workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                  # generate workflow.yml for site "local"
  %(prog)s -e condorpool                    # run the jobs on an HTCondor pool
  %(prog)s --submit                         # generate, plan, submit, and wait
  %(prog)s --container Quickstart_Container.sif
""",
    )

    # --- Standard Pegasus arguments ---
    parser.add_argument(
        "-s",
        "--skip-sites-catalog",
        action="store_true",
        help="Skip site catalog creation",
    )
    parser.add_argument(
        "-e",
        "--execution-site-name",
        metavar="STR",
        type=str,
        default="local",
        help="Execution site name (default: local, i.e. the submit host)",
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="STR",
        type=str,
        default="workflow.yml",
        help="Output file (default: workflow.yml)",
    )

    # --- Workflow-specific arguments ---
    parser.add_argument(
        "--input-file",
        metavar="PATH",
        type=str,
        default=None,
        help="Input file for the hello job (default: input/f.in, created if missing)",
    )
    parser.add_argument(
        "--spin-time",
        metavar="INT",
        type=int,
        default=3,
        help="Seconds each job spins to simulate work (default: 3)",
    )
    parser.add_argument(
        "--container",
        metavar="PATH",
        type=str,
        default=None,
        help="Run jobs inside this Apptainer .sif image (default: no container)",
    )
    parser.add_argument(
        "--submit",
        action="store_true",
        help="Plan and submit the workflow, then wait and print statistics",
    )

    args = parser.parse_args()

    wf_dir = str(Path(__file__).parent.resolve())
    input_file = args.input_file or os.path.join(wf_dir, "input", "f.in")

    # --- Input validation ---
    if args.input_file and not os.path.exists(args.input_file):
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)

    if args.container and not os.path.exists(args.container):
        logger.error(f"Container image not found: {args.container}")
        sys.exit(1)

    # The default input is a generated sample file — create it on first run.
    if not os.path.exists(input_file):
        os.makedirs(os.path.dirname(input_file), exist_ok=True)
        with open(input_file, "w") as f:
            f.write(DEFAULT_INPUT_CONTENTS)
        logger.info(f"Created sample input file: {input_file}")

    logger.info("=" * 70)
    logger.info("QUICKSTART (HELLO WORLD) WORKFLOW GENERATOR")
    logger.info("=" * 70)
    logger.info(f"Input file: {input_file}")
    logger.info(f"Execution site: {args.execution_site_name}")
    logger.info(f"Container: {args.container or 'none'}")
    logger.info(f"Output file: {args.output}")
    logger.info("=" * 70)

    try:
        workflow = QuickstartWorkflow(
            dagfile=args.output,
            input_file=input_file,
            container_image=args.container,
        )

        workflow.create_pegasus_properties()

        if not args.skip_sites_catalog:
            workflow.create_sites_catalog(exec_site_name=args.execution_site_name)

        workflow.create_transformation_catalog(
            exec_site_name=args.execution_site_name
        )
        workflow.create_replica_catalog()
        workflow.create_workflow(args)
        workflow.write()

        logger.info(f"\nWorkflow written to {args.output}")
        logger.info(
            f"Submit: pegasus-plan --submit "
            f"-s {args.execution_site_name} -o local {args.output}"
        )

    except Exception as e:
        logger.error(f"Failed to generate workflow: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    if not args.submit:
        return

    # --- Optional: plan, submit, and monitor from Python ---
    # These are wrappers around the pegasus-* CLI tools, so the same arguments
    # may be passed to them.
    try:
        workflow.wf.plan(
            sites=[args.execution_site_name],
            output_sites=["local"],
            output_dir=workflow.local_storage_dir,
            submit=True,
        )
    except PegasusClientError as e:
        logger.error(e)
        sys.exit(1)

    # Block until the workflow finishes, then report statistics.
    workflow.wf.wait()

    try:
        workflow.wf.statistics()
    except PegasusClientError as e:
        logger.error(e)


if __name__ == "__main__":
    main()
