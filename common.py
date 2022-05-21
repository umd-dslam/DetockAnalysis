import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StructType, StructField

from os.path import basename, dirname, isfile


def collect_col(sdf, col_name):
    return list(map(lambda r : r[col_name], sdf.collect()))
    

@udf("string")
def ancestor_udf(path, step=1):
    if step <= 0:
        return path
    for _ in range(step):
        path = dirname(path)
    return path


basename_udf = udf(basename, T.StringType())

def get_index(spark, prefix):
    client_sdf = spark.read.csv(f"{prefix}/*/client/*/metadata.csv", header=True)\
        .withColumn(
            "prefix",
            ancestor_udf(F.input_file_name(), lit(3))
        )\
        .dropDuplicates()

    server_sdf = spark.read.csv(f"{prefix}/*/server/*/metadata.csv", header=True)\
        .withColumn(
            "prefix",
            ancestor_udf(F.input_file_name(), lit(3))
        )\
        .dropDuplicates()

    return server_sdf.join(client_sdf, on='prefix')\
        .withColumn("duration", col("duration").cast(T.IntegerType()))\
        .withColumn("txns", col("txns").cast(T.IntegerType()))\
        .withColumn("clients", col("clients").cast(T.IntegerType()))\
        .withColumn("rate", col("rate").cast(T.IntegerType()))

#----------------------- client/*/transactions.csv -----------------------

def transactions_csv(spark, prefix, new_schema=False, start_offset_sec=0, duration_sec=1000000000):
    '''Reads client/*/transactions.csv files into a Spark dataframe'''
    
    regions_col_name = "regions" if new_schema else "replicas"

    transactions_schema = StructType([
        StructField("txn_id", T.LongType(), False),
        StructField("coordinator", T.IntegerType(), False),
        StructField(regions_col_name, T.StringType(), False),
        StructField("partitions", T.StringType(), False),
        StructField("generator", T.LongType(), False),
        StructField("restarts", T.IntegerType(), False),
        StructField("global_log_pos", T.StringType(), False),
        StructField("sent_at", T.DoubleType(), False),
        StructField("received_at", T.DoubleType(), False),
    ])

    sdf = spark.read.csv(
        f"{prefix}/client/*/transactions.csv",
        header=True,
        schema=transactions_schema
    )\
    .withColumn(
        regions_col_name,
        F.array_sort(F.split(regions_col_name, ";").cast(T.ArrayType(T.IntegerType())))
    )\
    .withColumn(
        "partitions",
        F.array_sort(F.split("partitions", ";").cast(T.ArrayType(T.IntegerType())))
    )\
    .withColumn(
        "machine",
        basename_udf(ancestor_udf(F.input_file_name())).cast(T.IntegerType())
    )\
    .withColumn(
        "min_sent_at",
        F.min("sent_at").over(Window.partitionBy("machine"))
    )\
    .withColumn(
        "max_sent_at",
        F.max("sent_at").over(Window.partitionBy("machine"))
    )\
    .withColumn(
        "global_log_pos",
        F.split("global_log_pos", ";").cast(T.ArrayType(T.IntegerType()))
    )\
    .where(
        (col("sent_at") >= col("min_sent_at") + start_offset_sec * 1000000000) &
        (col("sent_at") <= col("max_sent_at") + (start_offset_sec + duration_sec) * 1000000000)
    )

    return sdf


def summary_csv(spark, prefix):
    summary_schema = StructType([
        StructField("committed", T.LongType(), False),
        StructField("aborted", T.LongType(), False),
        StructField("not_started", T.LongType(), False),
        StructField("restarted", T.LongType(), False),
        StructField("single_home", T.LongType(), False),
        StructField("multi_home", T.LongType(), False),
        StructField("single_partition", T.LongType(), False),
        StructField("multi_partition", T.LongType(), False),
        StructField("remaster", T.LongType(), False),
        StructField("elapsed_time", T.LongType(), False)
    ])

    return spark.read.csv(
        f"{prefix}/client/*/summary.csv",
        header=True,
        schema=summary_schema
    )\
    .withColumn(
        "machine",
        basename_udf(ancestor_udf(F.input_file_name())).cast(T.IntegerType())
    )


def deadlocks_csv(spark, prefix, new_schema=False):
    region_col_name = "region" if new_schema else "replica"

    deadlocks_schema = StructType([
        StructField("time", T.LongType(), False),
        StructField("partition", T.IntegerType(), False),
        StructField(region_col_name, T.IntegerType(), False),
        StructField("vertices", T.IntegerType(), False),
    ])

    return spark.read.csv(
        f"{prefix}/server/*/deadlocks.csv",
        header=True,
        schema=deadlocks_schema
    )


def txn_timestamps_csv(spark, prefix):
    txn_timestamp_schema = StructType([
        StructField("txn_id", T.LongType(), False),
        StructField("from", T.IntegerType(), False),
        StructField("txn_timestamp", T.LongType(), False),
        StructField("server_time", T.LongType(), False)
    ])
    split_file_name = F.split(basename_udf(ancestor_udf(F.input_file_name())), '-')
    return spark.read.csv(
        f"{prefix}/server/*/txn_timestamps.csv",
        header=True,
        schema=txn_timestamp_schema
    )\
    .withColumn("dev", (col("txn_timestamp") - col("server_time")) / 1000000)\
    .withColumn("region", split_file_name[0].cast(T.IntegerType()))\
    .withColumn("partition", split_file_name[1].cast(T.IntegerType()))


def generic_csv(spark, prefix):
    generic_schema = StructType([
        StructField("type", T.IntegerType(), False),
        StructField("time", T.LongType(), False),
        StructField("data", T.LongType(), False),
        StructField("partition", T.IntegerType(), False),
        StructField("region", T.IntegerType(), False),
    ])

    return spark.read.csv(
        f"{prefix}/server/*/generic.csv",
        header=True,
        schema=generic_schema
    )


def committed(spark, prefix):
    return summary_csv(spark, prefix).select("committed").groupby().sum().collect()[0][0]


def sample_rate(spark, prefix, new_schema=False):
    sampled = transactions_csv(spark, prefix, new_schema).count()
    txns = committed(spark, prefix)
    return sampled / txns * 100 

    
def throughput(spark, prefix, new_schema=False, per_region=False, **kwargs):
    '''Computes throughput from client/*/transactions.csv file'''
    sample = sample_rate(spark, prefix, new_schema)
    throughput_sdf = transactions_csv(spark, prefix, new_schema, **kwargs)\
        .select("machine", col("sent_at").alias("time"))\
        .groupBy("machine")\
        .agg(
            (
                F.count("time") * 1000000000 * (100/sample) / (F.max("time") - F.min("time"))
            ).alias("throughput")
        )

    if per_region:
        return throughput_sdf
    else:
        return throughput_sdf.select(F.sum("throughput").alias("throughput"))


def latency(spark, prefixes, sample=1.0, new_schema=False, **kwargs):
    '''Compute latency from client/*/transactions.csv file'''
    latency_sdfs = []
    for p in prefixes:
        lat_sdf = transactions_csv(spark, p, new_schema, **kwargs).select(
            "txn_id",
            "coordinator",
            "regions" if new_schema else "replicas",
            "partitions",
            ((col("received_at") - col("sent_at")) / 1000000).alias("latency")
        )\
        .withColumn("prefix", lit(p))\
        .sample(sample)

        latency_sdfs.append(lat_sdf)

    assert len(latency_sdfs) > 0, "No latency data found"

    latency_sdf = latency_sdfs[0]
    for l in latency_sdfs[1:]:
        latency_sdf = latency_sdf.union(l)

    return latency_sdf


#----------------------- client/*/txn_events.csv -----------------------

def txn_events_csv(spark, prefix):
    '''Compute latency from client/*/transactions.csv file'''

    txn_events_schema = StructType([
        StructField("txn_id", T.LongType(), False),
        StructField("event", T.StringType(), False),
        StructField("time", T.DoubleType(), False),
        StructField("machine", T.IntegerType(), False),
        StructField("home", T.IntegerType(), False),
    ])

    return spark.read.csv(
        f"{prefix}/client/*/txn_events.csv",
        header=True,
        schema=txn_events_schema
    )


def events_latency(spark, prefix, from_event, to_event, new_col_name=None):
    '''Computes latency between a pair of events'''

    from_sdf = txn_events_csv(spark, prefix)\
        .where(col("event") == from_event)\
        .groupBy("txn_id", "machine")\
        .agg(F.max("time").alias("from_time"))
    to_sdf = txn_events_csv(spark, prefix)\
        .where(col("event") == to_event)\
        .groupBy("txn_id", "machine")\
        .agg(F.max("time").alias("to_time"))
    diff_col_name = new_col_name if new_col_name else f"{from_event}_to_{to_event}"
    return from_sdf.join(to_sdf, on=["txn_id", "machine"])\
        .select(
            "txn_id",
            "machine",
            ((col("to_time") - col("from_time")) / 1000000).alias(diff_col_name)
        )

#----------------------- server/*/events.csv -----------------------

def events_csv(spark, prefix, new_schema=False):
    '''Reads server/*/events.csv files into a Spark dataframe'''

    events_schema = StructType([
        StructField("txn_id", T.LongType(), False),
        StructField("event", T.StringType(), False),
        StructField("time", T.DoubleType(), False),
        StructField("partition", T.IntegerType(), False),
        StructField("region" if new_schema else "replica", T.IntegerType(), False),
    ])

    return spark.read.csv(
        f"{prefix}/server/*/events.csv",
        header=True,
        schema=events_schema
    )


def events_throughput(spark, prefix, sample, new_schema=False, per_machine=False):
    '''Counts number of events per time unit'''

    group_by_cols = ["event", "time"]
    if per_machine:
        group_by_cols += ["partition", "region" if new_schema else "replica"]

    return events_csv(spark, prefix, new_schema).withColumn(
        "time",
        (col("time") / 1000000000).cast(T.LongType())
    )\
    .groupBy(*group_by_cols)\
    .agg((F.count("txn_id")  * (100 / sample)).alias("throughput"))\
    .sort("time")


#----------------------- server/*/forw_sequ_latency.csv -----------------------

def fs_latency_csv(spark, prefix):
    '''Reads server/*/forw_sequ_latency.csv files into a Spark dataframe'''

    fs_latency_schema = StructType([
        StructField("dst", T.IntegerType(), False),
        StructField("send_time", T.LongType(), False),
        StructField("recv_time", T.LongType(), False),
        StructField("avg_latency", T.LongType(), False),
    ])

    return spark.read.csv(
        f"{prefix}/server/*/forw_sequ_latency.csv",
        header=True,
        schema=fs_latency_schema
    )\
    .withColumn("src", basename_udf(ancestor_udf(F.input_file_name())))


#----------------------- helper functions -----------------------

def remove_constant_columns(df, ignore=None):
    for c in df.columns:
        if ignore is not None and c in ignore:
            continue
        if len(df[c].unique()) == 1:
            df.drop(c, inplace=True, axis=1)
            
            
def normalize(col):
    min_val = np.nanmin(col.values)
    return col.values - min_val

            
def normalize2(col):
    min_val = np.nanmin(col.values)
    max_val = np.nanmax(col.values)
    return (col.values - min_val) / (max_val - min_val)


def compute_rows_cols(num_axes, num_cols=3):
    num_rows = num_axes // num_cols + (num_axes % num_cols > 0)
    return num_rows, num_cols


def from_cache_or_compute(cache_path, func, ignore_cache=False):
    if not ignore_cache and isfile(cache_path):
        return pd.read_parquet(cache_path)
    res = func()
    res.to_parquet(cache_path)        
    print(f"Saved to: {cache_path}")
    return res

#----------------------- plot functions -----------------------

def plot_cdf(a, ax=None, scale="log", **kargs):
    x = np.sort(a)
    y = np.arange(1, len(x)+1) / len(x)
    kargs.setdefault("markersize", 2)
    if ax is not None:
        ax.plot(x, y, **kargs)
        ax.set_xscale(scale)
    else:
        plt.plot(x, y, **kargs)
        plt.xscale(scale)


def plot_event_throughput(dfs, sharey=True, sharex=True, **kargs):
    events = [
        'ENTER_SERVER',
        'EXIT_SERVER_TO_FORWARDER',
        '',
        'ENTER_FORWARDER',
        'EXIT_FORWARDER_TO_SEQUENCER',
        'EXIT_FORWARDER_TO_MULTI_HOME_ORDERER',
        'ENTER_MULTI_HOME_ORDERER',
        'ENTER_MULTI_HOME_ORDERER_IN_BATCH',
        'EXIT_MULTI_HOME_ORDERER',
        'ENTER_SEQUENCER',
        'EXIT_SEQUENCER_IN_BATCH',
        '',
        'ENTER_LOG_MANAGER_IN_BATCH',
        'EXIT_LOG_MANAGER',
        '',
        'ENTER_SCHEDULER',
        'ENTER_SCHEDULER_LO',
        'ENTER_LOCK_MANAGER',
        'DISPATCHED_FAST',
        'DISPATCHED_SLOW',
        'DISPATCHED_SLOW_DEADLOCKED',
        'ENTER_WORKER',
        'EXIT_WORKER',
        '',
        'RETURN_TO_SERVER',
        'EXIT_SERVER_TO_CLIENT'
        '',
    ]
    num_rows, num_cols = compute_rows_cols(len(events), num_cols=3)
    _, axes = plt.subplots(num_rows, num_cols, sharey=sharey, sharex=sharex, figsize=(17, 25))
    
    for df in dfs.values():
        df.loc[:, 'time'] = normalize(df.loc[:, 'time'])
    
    for i, event in enumerate(events):
        r, c = i // num_cols, i % num_cols
        for label, df in dfs.items():
            df[df.event == event].plot(
                x="time",
                y="throughput",
                title=event.replace('_', ' '),
                marker='.',
                label=label,
                ax=axes[r, c]
            )
        axes[r, c].grid(axis='y')
        axes[r, c].set_xlabel('time')
        axes[r, c].set_ylabel('txn/sec')


    row_labels = [
        'SERVER',
        'FORWARDER',
        'MH_ORDERER',
        'SEQUENCER',
        'LOG_MANAGER',
        'SCHEDULER-1',
        'SCHEDULER-2',
        'WORKER',
        'SERVER',
    ]
    for ax, row in zip(axes[:,0], row_labels):
        ax.annotate(row, xy=(-50, 110), xytext=(0, 0),
                    xycoords="axes points", textcoords='offset points',
                    size='x-large', ha='right')
