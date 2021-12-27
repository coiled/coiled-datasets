import os
import dask

home = os.path.expanduser("~")

# 20 years

ddf_20y = dask.datasets.timeseries(
    start="2000-01-01",
    end="2020-12-31",
    freq="1s",
    partition_freq="7d",
    seed=42,
)

ddf_20y.to_parquet(
    f"{home}/data/timeseries/20-years/parquet/",
    compression="snappy",
    engine="pyarrow",
)

# 1 month

ddf_1m = dask.datasets.timeseries(
    start="2000-01-01",
    end="2000-01-31",
    freq="1s",
    partition_freq="7d",
    seed=42,
)

ddf_1m.to_parquet(
    f"{home}/data/timeseries/1-month/parquet/",
    compression="snappy",
    engine="pyarrow",
)

