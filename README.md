# Spark kernel for Cori@NERSC

## Create a kernel

Create custom jupyter kernels for using pyspark notebooks at NERSC.

Log on on Cori@NERSC, and run the script makekernel.py to create a kernel

```bash
python makekernel.py -kernelname <str> -spark_version <version> -pyspark_args <args>
```

Note that a startup script will be created, in order to load the Spark module
and launch a Spark cluster before launching the notebook:

```bash
#!/bin/bash
module load spark/<version>
start-all.sh
/usr/common/software/python/3.5-anaconda/bin/python -m ipykernel $@
```

The kernel will be stored at `$HOME/.ipython/kernels/`.
The following variables will be set:

```bash
"SPARK_HOME": "/path/to/spark",
"PYSPARK_SUBMIT_ARGS": <pyspark_args>,
"PYTHONSTARTUP": "/path/to/spark/python/pyspark/shell.py",
"PYTHONPATH": "/path/to/spark/python/lib/py4j-0.10.4-src.zip:/path/to/spark/python",
"PYSPARK_PYTHON": "/usr/common/software/python/3.5-anaconda/bin/python",
"PYSPARK_DRIVER_PYTHON": "ipython3"
```

You can choose Spark version 2.0.0 or 2.1.0 (later version are dealt with shifter
and support will come later). We support only Python 3.5 for the moment.

## JupyterLab: play with Apache Spark

Connect to https://jupyter-dev.nersc.gov/hub/login and create a notebook with
the kernel you just created:

<p align="center"><img width="600" src="https://github.com/astrolabsoftware/spark-kernel-nersc/raw/master/pic/load_kernel.png"/> </p>

<p align="center"><img width="600" src="https://github.com/astrolabsoftware/spark-kernel-nersc/raw/master/pic/spark_notebook.png"/> </p>
