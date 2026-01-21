# quickstart-workflow

This example will generate a simple Hello World workflow illustrated
below, then plan and execute the workflow. We will first run this
workflow locally, and then run on a HTCondor pool.

Rectangles represent input/output files, and ovals represent compute
jobs. The arrows represent file dependencies between each compute job.
This simple workflow will execute 2 jobs, each taking in one input file
and generating a single output file. The **world** job is dependant on
the output of the **hello**.

Each job in this workflow invokes a python executable named *hello* and
*world* . These are symlinks to the same executable *pegasus-keg.py*
. We use symbolic links in this case, to have the executable and the
job name match. The python code is a simple code that takes in an input
file; captures the hostname of the node where it is run on; and also
includes the contents of the input file in it's output.


![Hello World Workflow](./images/pipeline.svg)


The abstract workflow description that you specify to Pegasus is
portable, and usually does not contain any locations to physical input
files, executables or cluster end points where jobs are executed.

In this example, Pegasus will

* pick up the inputs from a directory named **input** .
* pick up the executables from a directory named **executables** .
* place the generated outputs in a directory named **output** .


## Run the Workflow

When working in Python, we can just use the reference do the `Workflow`
object, you can plan, run, and monitor the workflow directly. These are
wrappers around Pegasus CLI tools, and as such, the same arguments may
be passed to them.

    try:
        wf.plan(input_dirs=[INPUT_DIR], sites=[EXEC_SITE], transformations_dir=EXECUTABLES_DIR,\
                output_dir=OUTPUT_DIR, submit=True)
    except PegasusClientError as e:
        print(e)

Note the line in the output that starts with pegasus-status, contains
the command you can use to monitor the status of the workflow. We will
cover this command line tool in the next couple of notbooks. The path
it contains is the path to the submit directory where all of the files
required to submit and monitor the workflow are stored. For now we will
just continue to use the Python `Workflow` object

    wf.status(long=True)

We can also just block on the workflow finishing:

    wf.wait()


## Inspecting the generated output of the workflow

The executable that is run as part of this worklfow, is a simple python
script that captures the hostname, where a job ran, and also includes
the contents of the input file in it's output.

    cat output/f.out


## Statistics

Depending on if the workflow finished successfully or not, you
have options on what to do next. If the workflow failed you can
use `wf.analyze()` do get help finding out what went wrong. If the
workflow finished successfully, we can pull out some statistcs from the
provenance database:

    try:
        wf.statistics()
    except PegasusClientError as e:
        print(e)


## Run the workflow in a HTCondor Pool


Since we planned the workflow for a site named **local** the jobs ran on
submit host itself.

Now we will plan the same workflow again and have it run on a site named
**condorpool**.

Note, that we are not re-defining the workflow. The workflow to Pegasus
is described in a portable resource agnostic way. We are taking the same
abstract workflow and now, will replan it for a different execution
environment.

    try:
        wf.plan(input_dirs=[INPUT_DIR], sites=["condorpool"], transformations_dir=EXECUTABLES_DIR,\
                output_dir=OUTPUT_DIR, submit=True)\
          .wait()
    except PegasusClientError as e:
        print(e)

