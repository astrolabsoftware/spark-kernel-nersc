# Spark kernel for Cori@NERSC

## Create a kernel

Create custom jupyter kernels for using pyspark notebooks at NERSC.

Log on on cori, and run the script makekernel.py to create a kernel

```bash
python makekernel.py -kernelname mykernel -spark_version 2.1.0
```

It will set the spark env variable:

```bash
"SPARK_HOME": "/path/to/spark",
"PYSPARK_SUBMIT_ARGS": "--master <> --packages <> pyspark-shell",
"PYTHONSTARTUP": "/path/to/spark/python/pyspark/shell.py",
"PYTHONPATH": "/path/to/spark/python/lib/py4j-0.10.4-src.zip:/path/to/spark/python",
"PYSPARK_PYTHON": "/path/to/bin/python",
"PYSPARK_DRIVER_PYTHON": "ipython3"
```

You can choose spark version 2.0.0 or 2.1.0 (with python 3.5).

## Play with Apache Spark JupyterLab

Connect to https://jupyter-dev.nersc.gov/hub/login and create a notebook with the kernel mykernel${py_version} you just created:

_screenshot_
