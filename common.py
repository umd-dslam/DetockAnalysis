import os
import dask.dataframe as dd
import pandas as pd
import numpy as np

from matplotlib import pyplot as plt

CACHE_NAME = 'txn_time.parquet'

def load_txn_time(path, invalidate_cache=False):
    cache_path = os.path.join(path, CACHE_NAME)
    if not invalidate_cache and os.path.isfile(cache_path):
        return pd.read_parquet(cache_path)

    transactions_path = os.path.join(path, '*', '*', 'transactions.csv')
    transactions_df = dd.read_csv(transactions_path).set_index('txn_id')
    transactions_df = transactions_df.drop(['client_txn_id'], axis=1)

    event_names_path = os.path.join(path, '*', '*', 'event_names.csv')
    event_names_df = dd.read_csv(event_names_path).drop_duplicates().set_index('id')

    events_df = dd.read_csv(os.path.join(path, '*', '*', 'events.csv'))
    events_df = events_df.merge(event_names_df, left_on='event_id', right_index=True)
    events_df = events_df.drop_duplicates(subset=['txn_id', 'event', 'machine'])

    # Use this if pivot_table of Dask causes troubles
    events_df = events_df.compute()
    events_df = events_df.pivot(index=['txn_id', 'machine'], columns=['event'], values='time')
    
    # Use this if want to use pivot_table of Dask
    # events_df['txn_id_machine'] = events_df['txn_id'].astype(str) + ':' + events_df['machine'].astype(str)
    # events_df = events_df.drop(['txn_id', 'machine'], axis=1)
    # events_df['event'] = events_df['event'].astype('category').cat.as_known()
    # events_df = events_df.pivot_table(index='txn_id_machine', columns='event', values='time')
    # events_df = events_df.compute()
    
    events_df.reset_index(level='machine', inplace=True)
    txn_time_df = events_df.merge(transactions_df.compute(), left_on='txn_id', right_on='txn_id')
   
    txn_time_df.to_parquet(cache_path)
    
    return txn_time_df


def compute_rows_cols(num_axes, num_cols=3):
    num_rows = num_axes // num_cols + (num_axes % num_cols > 0)
    return num_rows, num_cols


def normalize(col):
    return (col - col.min()) / (col.max() - col.min())


def fill_common_values_and_remove_extras(txn_time_df):
    # Extract rows that belong to parts of each txn that reached the scheduler
    txn_time_parts_df = txn_time_df[txn_time_df['ENTER_SCHEDULER'].notna()]

    # Extract rows that only contain common data
    empty_txn_time_parts_df = txn_time_parts_df.drop(txn_time_parts_df.columns, axis=1)
    txn_time_common_server_df = txn_time_df[txn_time_df['ENTER_SERVER'].notna()].join(empty_txn_time_parts_df)
    txn_time_common_forwarder_df = txn_time_df[txn_time_df['ENTER_FORWARDER'].notna()].join(empty_txn_time_parts_df)
    txn_time_common_sequencer_df = txn_time_df[txn_time_df['ENTER_SEQUENCER'].notna()].join(empty_txn_time_parts_df)

    # Populate common data to the parts of each txn
    txn_time_parts_df = txn_time_parts_df.combine_first(txn_time_common_server_df)
    txn_time_parts_df = txn_time_parts_df.combine_first(txn_time_common_forwarder_df)
    txn_time_parts_df = txn_time_parts_df.combine_first(txn_time_common_sequencer_df)
    
    return txn_time_parts_df