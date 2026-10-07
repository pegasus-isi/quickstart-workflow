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

The site catalog (sites.yml) is managed by custom_sites.py: a sites.yml or
hosted catalog you provide is kept, and only missing entries are added.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from Pegasus.api import *

# Site-catalog handling shared with the standalone custom_sites.py script.
sys.path.insert(0, str(Path(__file__).parent.resolve()))
from custom_sites import (  # noqa: E402
    HOSTED_SITE, STYLES, ensure_sites_yml, hosted_catalog, parse_profile,
)

# Execution site when -e is not given: the submit host, unless ~/.pegasusrc
# names a hosted catalog (pegasushub pegasus-site-catalogs), whose one site is
# HOSTED_SITE ("compute").
DEFAULT_SITE = "local"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Per-tool resource configuration. Both jobs run the same tiny script, so the
# requirements are identical — kept as a table for consistency with the other
# workflows in this collection. runtime is the wall-clock budget in seconds:
# batch sites (Slurm through glite) require it and kill a job that exceeds it;
# condor pools ignore it. Everything else about where a job runs — scheduler,
# partition, account, scratch — belongs in the site catalog (custom_sites.py).
TOOL_CONFIGS = {
    "hello": {"memory": "1 GB", "cores": 1, "runtime": 600},
    "world": {"memory": "1 GB", "cores": 1, "runtime": 600},
}

DEFAULT_INPUT_CONTENTS = (
    "This is the contents of the input file for the hello world workflow!"
)


class QuickstartWorkflow:
    """Two-job Hello World workflow: hello -> world."""

    wf = None
    tc = None
    rc = None
    props = None

    dagfile = None
    wf_dir = None
    local_storage_dir = None
    wf_name = "hello-world"

    def __init__(self, dagfile="workflow.yml", input_file=None, container_image=None):
        self.dagfile = dagfile
        self.wf_dir = str(Path(__file__).parent.resolve())
        self.local_storage_dir = os.path.join(self.wf_dir, "output")
        self.input_file = input_file or os.path.join(self.wf_dir, "input", "f.in")
        self.container_image = container_image

    def write(self):
        """Write the catalogs and the workflow (sites.yml: custom_sites.py)."""
        self.props.write()
        self.rc.write()
        self.tc.write()
        self.wf.write(file=self.dagfile)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------
    def create_pegasus_properties(self, sites_yml="sites.yml",
                                  bypass_input_staging=False):
        """Planner properties.

        The site catalog itself is custom_sites.py's business; naming an
        existing sites.yml here lets pegasus-plan find it from any directory.
        """
        self.props = Properties()
        self.props["pegasus.transfer.threads"] = "16"
        # Symlink rather than copy when an input already sits on the
        # execution site. A no-op otherwise, so always on.
        self.props["pegasus.transfer.links"] = "true"
        if bypass_input_staging:
            # Jobs read inputs (scripts, the .sif image) straight from the
            # submit host's paths instead of through the staging site. Only
            # valid where workers share a filesystem with the submit host —
            # a Slurm cluster, typically; not a condor pool staging over
            # HTCondor file transfer.
            self.props["pegasus.transfer.bypass.input.staging"] = "true"
        if os.path.isfile(sites_yml):
            self.props["pegasus.catalog.site"] = "YAML"
            self.props["pegasus.catalog.site.file"] = os.path.abspath(sites_yml)

    # ------------------------------------------------------------------
    # Transformation Catalog
    # ------------------------------------------------------------------
    def create_transformation_catalog(self, bind_workflow_dir=False):
        """Containers and transformations; nothing here names a site.

        bind_workflow_dir: on a site that stages through its own filesystem
        (a Slurm cluster, a hosted catalog) or with bypass staging,
        pegasus.transfer.links stages inputs as symlinks to absolute paths
        under the workflow directory. PegasusLite starts the container with
        --no-home and binds only the job directory, so those links dangle
        inside it and every job dies with kickstart "Unable to execute the
        specified binary" (exit 127). Binding the workflow directory at its
        own path makes them resolve. Never on a condor pool: inputs arrive
        there as copies and the directory does not exist on the workers, so
        the bind would fail every job.
        """
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
            if bind_workflow_dir:
                container.add_pegasus_profile(
                    container_arguments=f"--bind {self.wf_dir}")
            self.tc.add_containers(container)

        # bin/hello.py and bin/world.py are symlinks to the same script,
        # bin/pegasus-keg.py — the job name and the executable name match, which
        # makes the job easy to spot in the logs. They are registered on
        # "local", where they live; Pegasus stages them to the execution site.
        transformations = []
        for tool_name, config in TOOL_CONFIGS.items():
            tx = Transformation(
                tool_name,
                site="local",
                pfn=os.path.join(self.wf_dir, f"bin/{tool_name}.py"),
                is_stageable=True,
                container=container,
            ).add_pegasus_profile(
                memory=config["memory"],
                cores=config["cores"],
                runtime=str(config["runtime"]),
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
# Site catalog
# ======================================================================
def setup_site_catalog(args, wf_dir):
    """Ensure the site catalog can plan args.execution_site; return its style.

    Defaults work untouched (an HTCondor site is added if nothing defines
    the requested one), a sites.yml or hosted catalog someone provided wins,
    and --site-style/--queue/--project/... tailor it for a batch cluster.
    """
    action, style = ensure_sites_yml(
        args.sites_yml, args.execution_site, wf_dir,
        style=args.site_style, queue=args.queue, project=args.project,
        scratch=args.site_scratch, profiles=args.site_profile)
    hosted = hosted_catalog()
    logger.info(f"Site catalog: {args.sites_yml}: {action}"
                + (f" (merged over hosted {hosted})" if hosted else ""))
    if style is None and hosted and args.execution_site != "local":
        logger.info(f"The hosted catalog {hosted} decides how "
                    f"{args.execution_site!r} submits; hosted catalogs name "
                    f"their site {HOSTED_SITE!r}.")
        if args.execution_site != HOSTED_SITE:
            # Nothing was written for this site, so planning works only if
            # the hosted catalog happens to define it.
            logger.warning(
                f"{args.execution_site!r} is not defined in {args.sites_yml} "
                f"and hosted catalogs normally define only {HOSTED_SITE!r}: "
                f"pegasus-plan will fail unless {hosted} has it. Use "
                f"-e {HOSTED_SITE}, or --site-style condor/slurm to describe "
                f"{args.execution_site!r}.")
    return style


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
  %(prog)s -e compute --site-style slurm --project my_lab   # hosted catalog
  %(prog)s --submit                         # generate, plan, submit, and wait
  %(prog)s --container Quickstart_Container.sif
""",
    )

    # --- Execution site. The workflow states only cores/memory/runtime;
    # these options shape the site catalog (custom_sites.py).
    parser.add_argument(
        "-e",
        "--execution-site",
        "--execution-site-name",
        dest="execution_site",
        metavar="STR",
        type=str,
        default=None,
        help=f"Site to plan against (default: {HOSTED_SITE!r} when "
             "~/.pegasusrc names a hosted catalog, which call their site "
             f"that; otherwise {DEFAULT_SITE!r}, i.e. the submit host)",
    )
    parser.add_argument(
        "--site-style",
        choices=("auto",) + STYLES + ("none",),
        default="auto",
        help="How the execution site is described in sites.yml. auto (default): "
             "keep a sites.yml entry or hosted catalog if one exists, else add "
             "an HTCondor site. condor/slurm: (re)write that site's entry. "
             "none: leave sites.yml alone.",
    )
    parser.add_argument(
        "--queue",
        metavar="PARTITION",
        help="Batch partition/queue jobs submit to (required for "
             "--site-style slurm without a hosted catalog)",
    )
    parser.add_argument(
        "--project",
        metavar="ACCOUNT",
        help="Allocation/account charged on a batch site",
    )
    parser.add_argument(
        "--site-scratch",
        metavar="DIR",
        help="Slurm only: shared scratch visible to workers and the submit "
             "host (default: ./work)",
    )
    parser.add_argument(
        "--site-profile",
        action="append",
        default=[],
        type=parse_profile,
        metavar="NS:KEY=VALUE",
        help="Extra profile on the execution site, e.g. "
             "pegasus:glite.arguments=--constraint=avx512; repeatable",
    )
    parser.add_argument(
        "--shared-filesystem",
        choices=("auto", "yes", "no"),
        default="auto",
        help="Let jobs read inputs (incl. a container image) directly from "
             "the submit host instead of via staging. auto (default): on for a "
             "Slurm site, off for HTCondor, which stages over file transfer.",
    )
    parser.add_argument(
        "--sites-yml",
        metavar="FILE",
        type=str,
        default="sites.yml",
        help="Local site catalog (default: sites.yml). Named in the generated "
             "properties, so pegasus-plan finds it from any directory.",
    )
    parser.add_argument(
        "-s",
        "--skip-sites-catalog",
        action="store_true",
        help="Deprecated: same as --site-style none",
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
    if args.execution_site is None:
        args.execution_site = HOSTED_SITE if hosted_catalog() else DEFAULT_SITE
    if args.skip_sites_catalog:
        args.site_style = "none"

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
    logger.info(f"Execution site: {args.execution_site}")
    logger.info(f"Container: {args.container or 'none'}")
    logger.info(f"Output file: {args.output}")
    logger.info("=" * 70)

    try:
        workflow = QuickstartWorkflow(
            dagfile=args.output,
            input_file=input_file,
            container_image=args.container,
        )

        style = setup_site_catalog(args, workflow.wf_dir)
        if args.shared_filesystem == "auto":
            bypass = style is not None and style != "condor"
        else:
            bypass = args.shared_filesystem == "yes"
        # A site that is not a condor pool stages through its own filesystem
        # (an unknown style over a hosted catalog counts: hosted catalogs are
        # batch sites), and then staged inputs are symlinks into wf_dir.
        batch_site = args.execution_site != "local" and (
            style not in (None, "condor")
            or (style is None and hosted_catalog() is not None))
        bind_wf = bool(args.container) and (batch_site or bypass)
        logger.info(
            "Input staging: "
            + ("bypassed (shared filesystem)" if bypass else "via staging site")
            + (f"; container binds {workflow.wf_dir}" if bind_wf else ""))

        workflow.create_pegasus_properties(
            sites_yml=args.sites_yml, bypass_input_staging=bypass)
        workflow.create_transformation_catalog(bind_workflow_dir=bind_wf)
        workflow.create_replica_catalog()
        workflow.create_workflow(args)
        workflow.write()

        logger.info(f"\nWorkflow written to {args.output}")
        logger.info(
            f"Submit: pegasus-plan --submit "
            f"-s {args.execution_site} -o local {args.output}"
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
            sites=[args.execution_site],
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
