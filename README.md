# coiled-datasets

Coiled hosts several public datasets in AWS S3 that you can easily query when experimenting with Dask.

Here's an example code snippet that creates a DataFrame with 662 million rows of data:

```python

ddf = dd.read_parquet(
    "s3://coiled-datasets/timeseries/20-years/parquet",
    storage_options={"anon": True, "use_ssl": True}
)
```

These easily accessible datasets make it a lot easier for you to run Dask analyses and perform benchmarking analyses.

Here are some key facts on the datasets:

## timeseries

The timeseries datasets are created with `dask.datasets.timeseries` and have the following schema:

```
id        int64
name     object
x       float64
y       float64
```

### timeseries/20-years/parquet

* Description: Data from 2000 to 2021 with one row every second
* Uncompressed size: 58.2 GB
* Compressed size: 16.7 GB
* Number files: 1,097
* Number rows: 662,256,000

### Arcos Opioid Sales Dataset

* Description: Arcos Opioid Sales Dataset as released by the Washington Post. 
* Uncompressed size (tsv): 74.5GB
* Compressed size (parquet): 7.9 GB
* Number files: 3,752
* Number rows: 178,598,026

* Source: https://www.washingtonpost.com/national/2019/07/18/how-download-use-dea-pain-pills-database/)
* Downloaded on: September 20, 2021
* Dataset description and metadata: https://github.com/wpinvestigative/arcos-api/
* Note: The original dataset is stored in .tsv format. This dataset has been preprocessed into the more efficient Parquet file format.

Here's an example code snippet that creates a Dask DataFrame by reading in the entire Parquet file from the `coiled-datasets` S3 bucket:

```
# download data from S3
data = dd.read_parquet(
    "s3://coiled-datasets/dea-opioid/arcos_washpost_comp.parquet",
    compression="lz4",
    storage_options={"anon": True},
)
```

## Why use these datasets

A lot of open source datasets aren't easily accessible.  You need to download them from a website which can be slow.

Some datasets are stored in inefficient file formats like CSV.  They're not stored in Parquet files that have schema metadata information and allow for performance enhancements like column pruning.

Some open source datasets are also messy, so you need to clean the data before performing your analysis.  Suppose you'd like to try out a new machine learning model on a large dataset.  You may not want to perform hours cleaning up the data if your end goal is to experiment with some models.

## Localhost datasets

This repo also has scripts for you to create example datasets on your local machine.

See the `create-scripts` directory.

For example, you can run `python create-scripts/timeseries.py` to create some of the timeseries datasets on your local machine.  They'll be written in the `~/data` directory.

