#!/usr/bin/python
# Copyright (C) 2018 Julien Peloton
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
"""
Generate Jupyter kernels to use desc-python + Apache Spark at NERSC

Author: Julien Peloton, peloton@lal.in2p3.fr
"""
import os
import stat
import argparse

from kernel_util import safe_mkdir

def create_desc_startup_file(path, pyspark_args):
    """
    Create a startup file (bash) to load DESC environment

    Parameters
    ----------
    path : str
        Where to store the startup file
    pyspark_args: str
        Extra arguments to pass to pyspark. Typically:
        --master local[n] --packages <> --jars <>
        See https://spark.apache.org/docs/latest/submitting-applications.html
        for more information.

    Returns
    ----------
    filename: str
        Returns the startup script filename (with full path)
    """
    filename = os.path.join(path, 'start_desc.sh')

    startup = """#!/bin/bash
# Where the Spark logs will be stored
# Logs can be then be browsed from the Spark UI
LOGDIR=${{SCRATCH}}/spark/event_logs
mkdir -p ${{LOGDIR}}

# The directory `/global/cscratch1/sd/<user>/tmpfiles` will be created if it
# does not exist to store temporary files used by Spark.
mkdir -p ${{SCRATCH}}/tmpfiles

# Path to LSST miniconda installation at NERSC
lSSTCONDA="/global/common/software/lsst/common/miniconda"

# Since the default NERSC Apache Spark runs inside of Shifter, we use
# a custom local version of it. This is maintained by me (Julien Peloton)
# at NERSC. If you encounter problems, let me know (peloton at lal.in2p3.fr)!
SPARKPATH="/global/homes/p/peloton/myspark/spark-2.3.2-bin-hadoop2.7"

# Here is the environment needed for Spark to run at NERSC.
export SPARK_HOME="${{SPARKPATH}}"
export PYSPARK_SUBMIT_ARGS="{} pyspark-shell"
export PYTHONSTARTUP="${{SPARKPATH}}/python/pyspark/shell.py"

# Make sure the version of py4j is correct.
export DESCPYTHONPATH="${{SPARKPATH}}/python/lib/py4j-0.10.7-src.zip:${{SPARKPATH}}/python"

# Should correspond to desc-python
export PYSPARK_PYTHON="${{lSSTCONDA}}/current/bin/python"
export PYSPARK_DRIVER_PYTHON="${{lSSTCONDA}}/current/bin/ipython3"

# We use Java 8. Spark 2+ does not work with Java 7 and earlier versions.
export JAVA_HOME="/opt/java/jdk1.8.0_51"

# desc-python activation script
source ${{lSSTCONDA}}/kernels/python.sh
    """.format(pyspark_args)

    with open(filename, 'w') as f:
        print(startup, file=f)

    # Change permission to rwx for the user
    os.chmod(filename, stat.S_IRWXU)

    return filename

def create_desc_kernel(path, startupname, kernelname):
    """
    Create a Kernel file with python DESC + Spark env.
    The Spark version is 2.3.2, maintained by J. Peloton at NERSC.

    Parameters
    ----------
    path : str
        Where to store the kernel file
    startupname : str
        Startup script filename (with full path) which load Spark module,
        and start the cluster. See `create_startup_file`.
    kernelname : str
        Name of the kernel (will be displayed on the UI).
    """
    # Kernel path + name
    filename = os.path.join(path, 'kernel.json')

    # Kernel file
    kernel = """{{
  "display_name": "{} (2.3.2)",
  "language": "python",
  "argv": [
    "{}",
    "-f",
    "{{connection_file}}"],
}}
    """.format(kernelname, startupname)

    with open(filename, 'w') as f:
        print(kernel, file=f)

def addargs(parser):
    """ Parse command line arguments for desc-python-spark kernel creation """
    parser.add_argument(
        '-kernelname', dest='kernelname',
        required=True,
        help='Name of the Jupyter kernel to be displayed')

    parser.add_argument(
        '-pyspark_args', dest='pyspark_args',
        default="--master local[4]",
        help="""
        Submission arguments for pyspark.
        See https://spark.apache.org/docs/latest/submitting-applications.html
        for more information. Default is "--master local[4]".
        """)

    parser.add_argument(
        '--local', dest='local',
        action="store_true",
        help="""
        If specified, kernel and startup scripts will be dump on the current
        working directory instead of the NERSC kernel folder. Default is False.
        """)


if __name__ == "__main__":
    """ Create Jupyter kernels for using desc-python + Apache Spark at NERSC

    Launch it using `python desc-kernel.py <args>`.

    Run `python desc-kernel.py --help` for more information on inputs.
    """
    parser = argparse.ArgumentParser(
        description='Create a desc-python-spark kernel to work at NERSC')
    addargs(parser)
    args = parser.parse_args(None)

    msg = """
    Note:
    ----------------
    The large-memory login node used by https://jupyter-dev.nersc.gov/
    is a shared resource, so please be careful not to use too many CPUs
    or too much memory.

    That means do not specify `--master local[*]` in your kernel, but limit
    the resource to a few core. Typically `--master local[4]` is enough for
    prototyping a program.
    """
    print(msg)

    # Grab $HOME path
    HOME = os.environ['HOME']

    if not args.local:
        # Kernels are stored here
        # See https://jupyter-client.readthedocs.io/en/stable/kernels.html#kernel-specs
        path = '{}/.local/share/jupyter/kernels/{}'.format(HOME, args.kernelname)
    else:
        path = os.curdir

    # Create the folder to store the kernel if needed,
    # and store the kernel + the startup script
    safe_mkdir(path, verbose=True)

    valid = True
    startup_fn = create_desc_startup_file(path, args.pyspark_args)

    # Create the kernel
    create_desc_kernel(path, startup_fn, args.kernelname)
    print("Kernel stored at {}".format(path))
