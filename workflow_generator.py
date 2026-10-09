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
    ./workflow_generator.py                       # jobs on site "compute"
    ./workflow_generator.py -e condorpool         # plain HTCondor pool, no site catalog
    ./workflow_generator.py -s access-pegasus.yml # a hosted site catalog
    ./workflow_generator.py -e local              # on the submit host

Sites follow pegasus-isi/pegasus-gromacs: jobs run on a site named "compute",
defined by a centrally hosted site catalog (-s FILE, or one in ~/.pegasusrc;
https://github.com/pegasushub/pegasus-site-catalogs). The generator writes no
site catalog and never submits: it prints the pegasus-plan command, and the
notebook (Quickstart-Workflow.ipynb) submits from an explicit cell.
"""

import argparse
import logging
import os
import subprocess
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

# Pegasus worker package (kickstart etc.) used *inside* the optional
# container, which is Debian 13 (python:3.11-slim-trixie) whatever the submit
# host runs. Left alone, PegasusLite sees the submit host's package as a
# mismatch and tries to download a deb_13 one from inside the container, which
# has no curl/wget (exit code 71). PEGASUS.md "Worker package in containers".
# Change this with the container's base image.
WORKER_PACKAGE_PLATFORM = "x86_64_deb_13"
WORKER_PACKAGE_URL = ("https://download.pegasus.isi.edu/pegasus/{v}/"
                      "pegasus-worker-{v}-" + WORKER_PACKAGE_PLATFORM + ".tar.gz")

DEFAULT_INPUT_CONTENTS = (
    "This is the contents of the input file for the hello world workflow!"
)


def planner_version():
    """Version of the pegasus-plan that will plan this workflow, or None."""
    try:
        out = subprocess.run(["pegasus-version"], capture_output=True,
                             text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None
    version = out.stdout.strip()
    return version if out.returncode == 0 and version else None


def ensure_input_file(input_file):
    """Create the default sample input on first run; return its path."""
    if not os.path.exists(input_file):
        os.makedirs(os.path.dirname(input_file), exist_ok=True)
        with open(input_file, "w") as f:
            f.write(DEFAULT_INPUT_CONTENTS)
        logger.info(f"Created sample input file: {input_file}")
    return input_file


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
        # Only the container needs its own worker package (see above).
        self.worker_package_url = None
        if container_image:
            version = planner_version()
            if version:
                self.worker_package_url = WORKER_PACKAGE_URL.format(v=version)
            else:
                logger.warning(
                    "pegasus-version not found; the container will try to "
                    "download a worker package itself and fail (no curl/wget)")

    def write(self):
        """Write all catalogs and workflow to files."""
        if self.sc is not None:
            self.sc.write()
        self.props.write()
        self.rc.write()
        self.tc.write()
        self.wf.write(file=self.dagfile)

    # ------------------------------------------------------------------
    # Plan / run / monitor (thin wrappers over the Pegasus API Workflow
    # object, for interactive use e.g. from a Jupyter notebook)
    # ------------------------------------------------------------------
    def plan_submit(self, exec_site_name="compute", raise_errors=False):
        try:
            self.wf.plan(
                dir="submit",
                sites=[exec_site_name],
                output_sites=["local"],
                cleanup="none",
                verbose=1,
                submit=True,
            )
        except PegasusClientError as e:
            print(e)
            if raise_errors:
                raise

    def status(self):
        try:
            self.wf.status(long=True)
        except PegasusClientError as e:
            print(e)

    def wait(self):
        try:
            self.wf.wait()
        except PegasusClientError as e:
            print(e)

    def statistics(self):
        try:
            self.wf.statistics()
        except PegasusClientError as e:
            print(e)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------
    def create_pegasus_properties(self, hosted_site_catalog=None):
        self.props = Properties()
        self.props["pegasus.transfer.threads"] = "16"
        if hosted_site_catalog:
            # Use one of Pegasus' centrally hosted site catalogs instead of
            # a locally generated one. pegasus-plan downloads and caches the
            # named file from the catalog repository at plan time.
            # https://pegasus.isi.edu/documentation/reference-guide/catalogs.html#centrally-hosted-site-catalogs
            self.props["pegasus.catalog.site.repo.file"] = hosted_site_catalog
        if self.worker_package_url:
            # Stage the container's worker package named in the transformation
            # catalog and never download one from inside the job.
            self.props["pegasus.transfer.worker.package"] = "true"
            self.props["pegasus.transfer.worker.package.strict"] = "false"
            self.props["pegasus.transfer.worker.package.autodownload"] = "false"

    # ------------------------------------------------------------------
    # Site Catalog
    #
    # Not used by the CLI below by default — pegasus-plan resolves the site
    # catalog from a centrally hosted one instead (see -s/--hosted-site-catalog
    # and create_pegasus_properties above). Kept for programmatic/notebook use
    # when a self-contained, locally generated HTCondor site catalog is wanted.
    # ------------------------------------------------------------------
    def create_sites_catalog(self, exec_site_name="compute"):
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

        exec_site = (
            Site(exec_site_name)
            .add_condor_profile(universe="vanilla")
            .add_pegasus_profile(style="condor")
        )

        self.sc.add_sites(local, exec_site)

    # ------------------------------------------------------------------
    # Transformation Catalog
    # ------------------------------------------------------------------
    def create_transformation_catalog(self, exec_site_name="compute"):
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
            if self.worker_package_url:
                self.tc.add_transformations(
                    Transformation(
                        "worker",
                        namespace="pegasus",
                        site="local",
                        pfn=self.worker_package_url,
                        is_stageable=True,
                        arch=Arch.X86_64,
                        os_type=OS.LINUX,
                    )
                )

        # bin/hello.py and bin/world.py are symlinks to the same script,
        # bin/pegasus-keg.py — the job name and the executable name match, which
        # makes the job easy to spot in the logs. Pegasus stages them from the
        # submit host to the execution site.
        transformations = []
        for tool_name, config in TOOL_CONFIGS.items():
            tx = Transformation(
                tool_name,
                site=exec_site_name,
                pfn=os.path.join(self.wf_dir, f"bin/{tool_name}.py"),
                is_stageable=True,
                container=container,
            ).add_pegasus_profile(
                memory=config["memory"], cores=config.get("cores", 1)
            )
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
  %(prog)s                              # generate workflow.yml for site "compute"
  %(prog)s -e condorpool                # plain HTCondor pool, no site catalog
  %(prog)s -s access-pegasus.yml        # a centrally hosted site catalog
  %(prog)s -e local                     # run the jobs on the submit host
  %(prog)s --container Apptainer/Quickstart_Container.sif

Writes the workflow and its catalogs; it does not plan or submit. Plan with
the command it prints, or from the notebook (plan_submit()).
""",
    )

    parser.add_argument(
        "-s",
        "--hosted-site-catalog",
        metavar="FILE",
        type=str,
        default=None,
        help="Name of a Pegasus centrally hosted site catalog to plan against "
        "(e.g. access-pegasus.yml), instead of a locally generated one. Sets "
        "pegasus.catalog.site.repo.file; see "
        "https://pegasus.isi.edu/documentation/reference-guide/catalogs.html"
        "#centrally-hosted-site-catalogs",
    )
    parser.add_argument(
        "-e",
        "--execution-site-name",
        metavar="STR",
        type=str,
        default="compute",
        help="Execution site name (default: compute; condorpool on a plain "
        "HTCondor pool with no site catalog)",
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
    ensure_input_file(input_file)

    logger.info("=" * 70)
    logger.info("QUICKSTART (HELLO WORLD) WORKFLOW GENERATOR")
    logger.info("=" * 70)
    logger.info(f"Input file: {input_file}")
    logger.info(f"Execution site: {args.execution_site_name}")
    logger.info(
        f"Hosted site catalog: {args.hosted_site_catalog or '(none — supply your own site catalog)'}"
    )
    logger.info(f"Container: {args.container or 'none'}")
    logger.info(f"Output file: {args.output}")
    logger.info("=" * 70)

    try:
        workflow = QuickstartWorkflow(
            dagfile=args.output,
            input_file=input_file,
            container_image=args.container,
        )

        workflow.create_pegasus_properties(hosted_site_catalog=args.hosted_site_catalog)
        workflow.create_transformation_catalog(exec_site_name=args.execution_site_name)
        workflow.create_replica_catalog()
        workflow.create_workflow(args)
        workflow.write()

        logger.info(f"\nWorkflow written to {args.output}")
        logger.info(
            f"Plan and submit: pegasus-plan --dir submit "
            f"-s {args.execution_site_name} -o local --submit {args.output}"
        )

    except Exception as e:
        logger.error(f"Failed to generate workflow: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
