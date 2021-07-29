import matplotlib.pyplot as plt
import numpy as np
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StructType, StructField

from os.path import basename, dirname

@udf("string")
def ancestor_udf(path, step=1):
    if step <= 0:
        return path
    for _ in range(step):
        path = dirname(path)
    return path


basename_udf = udf(basename, T.StringType())

#----------------------- client/*/transactions.csv -----------------------

def transactions_csv(spark, prefix, trim_start_sec=0, trim_end_sec=0, new_version=True):
    '''Reads client/*/transactions.csv files into a Spark dataframe'''
    
    if new_version:
        transactions_schema = StructType([
            StructField("txn_id", T.LongType(), False),
            StructField("coordinator", T.IntegerType(), False),
            StructField("replicas", T.StringType(), False),
            StructField("partitions", T.StringType(), False),
            StructField("generator", T.LongType(), False),
            StructField("restarts", T.IntegerType(), False),
            StructField("global_log_pos", T.StringType(), False),
            StructField("sent_at", T.DoubleType(), False),
            StructField("received_at", T.DoubleType(), False),
        ])
    else:
        transactions_schema = StructType([
            StructField("txn_id", T.LongType(), False),
            StructField("coordinator", T.IntegerType(), False),
            StructField("replicas", T.StringType(), False),
            StructField("partitions", T.StringType(), False),
            StructField("generator", T.LongType(), False),
            StructField("restarts", T.IntegerType(), False),
            StructField("sent_at", T.DoubleType(), False),
            StructField("received_at", T.DoubleType(), False),
        ])

    sdf = spark.read.csv(
        f"{prefix}/client/*/transactions.csv",
        header=True,
        schema=transactions_schema
    )\
    .withColumn(
        "replicas",
        F.array_sort(F.split("replicas", ";").cast(T.ArrayType(T.IntegerType())))
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
        "min_received_at",
        F.min("received_at").over(Window.partitionBy("machine"))
    )\
    .withColumn(
        "max_received_at",
        F.max("received_at").over(Window.partitionBy("machine"))
    )\
    .where(
        (col("received_at") >= col("min_received_at") + trim_start_sec * 1000000000) &
        (col("received_at") <= col("max_received_at") - trim_end_sec * 1000000000)
    )
    
    if new_version:
        sdf = sdf.withColumn(
            "global_log_pos",
            F.array_sort(F.split("global_log_pos", ";").cast(T.ArrayType(T.IntegerType())))
        )

    return sdf


def throughput(spark, prefix, sample, per_region=False, **kwargs):
    '''Computes throughput from client/*/transactions.csv file'''

    throughput_sdf = transactions_csv(spark, prefix, **kwargs)\
        .select("machine", col("received_at").alias("time"))\
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


def latency(spark, prefix, percentage=None, where=None, **kwargs):
    '''Compute latency from client/*/transactions.csv file'''

    latency_sdf = transactions_csv(spark, prefix, **kwargs).select(
        "txn_id",
        "coordinator",
        "replicas",
        "partitions",
        ((col("received_at") - col("sent_at")) / 1000000).alias("latency")
    )
    if where is not None:
        latency_sdf = latency_sdf.where(where)
    if not percentage:
        return latency_sdf
    return latency_sdf.select(
        F.percentile_approx("latency", percentage, 100000).alias("percentiles")
    )


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

def events_csv(spark, prefix):
    '''Reads server/*/events.csv files into a Spark dataframe'''

    events_schema = StructType([
        StructField("txn_id", T.LongType(), False),
        StructField("event", T.StringType(), False),
        StructField("time", T.DoubleType(), False),
        StructField("partition", T.IntegerType(), False),
        StructField("replica", T.IntegerType(), False),
    ])

    return spark.read.csv(
        f"{prefix}/server/*/events.csv",
        header=True,
        schema=events_schema
    )


def events_throughput(spark, prefix, sample, per_machine=False):
    '''Counts number of events per time unit'''

    group_by_cols = ["event", "time"]
    if per_machine:
        group_by_cols += ["partition", "replica"]

    return events_csv(spark, prefix).withColumn(
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


#----------------------- plot functions -----------------------

def plot_cdf(ax, a, scale="log", **kargs):
    x = np.sort(a)
    y = np.arange(1, len(x)+1) / len(x)
    kargs.setdefault("markersize", 2)
    kargs.setdefault("marker", 'o')
    ax.plot(x, y, **kargs)
    ax.set_xscale(scale)


def plot_event_throughput(dfs, **kargs):
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
        'ENTER_INTERLEAVER_IN_BATCH',
        'EXIT_INTERLEAVER',
#         'ENTER_LOG_MANAGER_IN_BATCH',
#         'EXIT_LOG_MANAGER',
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
    _, axes = plt.subplots(num_rows, num_cols, sharey=True, sharex=True, figsize=(17, 25))
    
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