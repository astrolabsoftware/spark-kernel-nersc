#!/usr/bin/python
"""
Script to generate Jupyter kernels to use Apache Spark at NERSC

Author: Julien Peloton, peloton@lal.in2p3.fr
"""
import os
import argparse

def safe_mkdir(path, verbose=False):
    """
    Create a folder and catch the race condition between path exists and mkdir.

    Parameters
    ----------
    path : string
        Name of the folder to be created (can be full path).
    verbose : bool
        If True, print messages about the status.

    Examples
    ----------
    Folders are created
    >>> safe_mkdir('toto/titi/tutu', verbose=True)

    Folders aren't created because they already exist
    >>> safe_mkdir('toto/titi/tutu', verbose=True)
    Folders toto/titi/tutu already exist. Not created.

    >>> os.removedirs('toto/titi/tutu')
    """
    abspath = os.path.abspath(path)
    if not os.path.exists(abspath):
        try:
            os.makedirs(abspath)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
    else:
        if verbose:
            print("Folders {} already exist. Not created.".format(path))

def create_startup_file(path, spark_version):
    """
    Create a startup file (bash) to load Spark module, and start a cluster.

    Parameters
    ----------
    path : str
        Where to store the startup file
    spark_version: str
        Apache Spark version. Only non-shifter versions are supported, that is
        version 2.0.0 and 2.1.0.

    Returns
    ----------
    filename: str
        Returns the startup script filename (with full path)
    """
    filename = os.path.join(path, 'start_spark.sh')

    pythonpath = "/usr/common/software/python/3.5-anaconda/bin/python"

    with open(filename, 'w') as f:
        print('#!/bin/bash', file=f)
        print('module load spark/{}'.format(spark_version), file=f)
        print('start-all.sh', file=f)
        print('{} -m ipykernel $@'.format(pythonpath), file=f)

    return filename


def create_kernel(path, startupname, kernelname, spark_version, pyspark_args):
    """
    Create a JSON file with the kernel properties.

    Parameters
    ----------
    path : str
        Where to store the kernel file
    startupname : str
        Startup script filename (with full path) which load Spark module,
        and start the cluster. See `create_startup_file`.
    kernelname : str
        Name of the kernel (will be displayed on the UI).
    spark_version: str
        Apache Spark version. Only non-shifter versions are supported, that is
        version 2.0.0 and 2.1.0.
    pyspark_args: str
        Extra arguments to pass to pyspark. Typically:
        --master local[n] --packages <> --jars <>
        See https://spark.apache.org/docs/latest/submitting-applications.html
        for more information.
    """
    software_path = "/global/common/cori/software"
    spark_path = "{}/spark/{}".format(software_path, spark_version)

    filename = os.path.join(path, 'kernel.json')

    with open(filename, 'w') as f:
        print('{', file=f)

        # Displayed name of the cluster
        print('  "display_name": "{} ({})",'.format(
            kernelname, spark_version), file=f)

        # Kernel language is Python
        print('  "language": "python",', file=f)

        # Startup args
        print('  "argv": [', file=f)

        # Startup script to launch the Spark cluster
        print('    "{}",'.format(startupname), file=f)

        # Other required args to start the kernel
        print('    "-m",', file=f)
        print('    "ipykernel",', file=f)
        print('    "-f",', file=f)
        print('    "{connection_file}"', file=f)
        print('  ],', file=f)

        # Environment
        print('  "env": {', file=f)

        # Spark installation path
        print('    "SPARK_HOME": "{}",'.format(spark_path), file=f)

        # Arguments to be passed to pyspark
        print('    "PYSPARK_SUBMIT_ARGS": ', file=f)

        # --> Resources
        print('    "{} pyspark-shell",'.format(pyspark_args), file=f)

        # Pyspark startup shell script
        print('    "PYTHONSTARTUP": "{}/python/pyspark/shell.py",'.format(
            spark_path), file=f)

        # Need to include py4j library
        print('    "PYTHONPATH":', file=f)
        print('    "{}/python,{}/python/lib/py4j-0.10.4-src.zip",'.format(
            spark_path, spark_path), file=f)

        # Version of Python. Only work for 3.5 for the moment.
        print('    "PYSPARK_PYTHON": ', file=f)
        print('"/usr/common/software/python/3.5-anaconda/bin/python",', file=f)

        # Ipython driver
        print('    "PYSPARK_DRIVER_PYTHON": "ipython3"', file=f)

        print('  }', file=f)
        print('}', file=f)


def addargs(parser):
    """ Parse command line arguments for spark-kernel-nersc """
    parser.add_argument(
        '-kernelname', dest='kernelname',
        required=True,
        help='Name of the Jupyter kernel to be displayed')

    parser.add_argument(
        '-spark_version', dest='spark_version',
        default="2.1.0",
        help="""
        Version of Apache Spark. Currently, only version
        2.0.0 and 2.1.0 are supported. Default is 2.1.0.
        """)

    parser.add_argument(
        '-pyspark_args', dest='pyspark_args',
        default="--master local[*]",
        help="""
        Submission arguments for pyspark.
        See https://spark.apache.org/docs/latest/submitting-applications.html
        for more information. Default is "--master local[*]".
        """)

    parser.add_argument(
        '--local', dest='local',
        action="store_true",
        help="""
        If specified, kernel and startup scripts will be dump on the current
        working directory instead of the NERSC kernel folder. Default is False.
        """)


if __name__ == "__main__":
    """ Create Jupyter kernels for using Apache Spark at NERSC

    Launch it using `python makekernel.py <args>`.

    Run `python makekernel.py --help` for more information on inputs.
    """
    parser = argparse.ArgumentParser(
        description='Create Jupyter kernels for using Apache Spark at NERSC')
    addargs(parser)
    args = parser.parse_args(None)

    # Grab $HOME path
    HOME = os.environ['HOME']

    if not args.local:
        # Kernels are stored here
        # See http://www.nersc.gov/users/data-analytics/
        # data-analytics-2/jupyter-and-rstudio/
        path = '{}/.ipython/kernels/{}'.format(HOME, args.kernelname)
    else:
        path = os.curdir

    # Create the folder to store the kernel if needed,
    # and store the kernel + the startup script
    safe_mkdir(path, verbose=True)

    startup_fn = create_startup_file(path, args.spark_version)
    create_kernel(
        path, startup_fn, args.kernelname,
        args.spark_version, args.pyspark_args)

    print("Kernel stored at {}".format(path))
