import datetime
import pandas as pd
import numpy as np
from google.cloud import bigquery, storage
import io
import re

# Function to count the number of words
def word_count(text):
    return len(re.findall(r'\w+', text))

def convert_time(time):
    return datetime.datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')


def sum_pvs(df, cat_col_name, prefix='', suffix=''):  
    """Pivot on user and adds prefix/suffix"""
    df_piv = pd.pivot_table(
        df, 
        values='GA_pageViews',
        columns=cat_col_name,
        index='GA_fullVisitorId',
        aggfunc='sum'
    ).reset_index().fillna(0)
    
    # Clean the columns and add in a prefix and or suffix
    df_piv.columns = df_piv.columns.map(lambda x : prefix + col_name(x) + suffix if x !='GA_fullVisitorId' else x)

    return df_piv


def calc_top(df, cat_col_name, prefix='', suffix=''):
    """
    Pivot on user and calc avg top by cat col 
    and adds a prefix/suffix
    """
    df_cat = pd.pivot_table(
        df, 
        values=['timeOnPage', 'GA_pageViews'],
        columns=cat_col_name,
        index='GA_fullVisitorId',
        aggfunc='sum'
    ).reset_index()
    
    # Set aside fvids
    fvids = list(df_cat.GA_fullVisitorId)

    # Calc avg. top for each category
    df_top = df_cat['timeOnPage'] / df_cat['GA_pageViews']
    
    # Fillna
    df_top = df_top.fillna(0)

    # Add back fvids
    df_top['GA_fullVisitorId'] = fvids
    
    # Clean the columns and add in a prefix and or suffix
    df_top.columns = df_top.columns.map(lambda x : prefix + str(col_name(x)) + suffix if x !='GA_fullVisitorId' else x)
    
    return df_top


def col_name(x):
    new=str(x).replace("(", "_")
    new=new.replace(")", "")
    new=new.replace(" & ", "_and_")
    new=new.replace(" ", "_")
    new=new.replace("-", "_")
    new=new.replace("'s", "")
    new=new.replace("'", "")
    new=new.replace("/", "")
    return new.lower()


def multiple_df_merge(df_list, on, how): 
    merged=df_list[0]
    for df_ in df_list[1:]:    
        merged=merged.merge(df_, on=on, how=how)
    return merged


def b_rate(g):
    '''For each fvid: calculate percentage of sessions comprising only 1PV'''
    # Count sessions w/ pv = 1
    sessions_w_1pv=g[g['session_pvs']==1].shape[0]
    
    # Count total sessions
    total_sessions=g.shape[0]
    
    # Calculate ratio
    return (sessions_w_1pv) / total_sessions


def c_views_rate(g):
    '''
    For each fvid: 
        Calculate percentage PVs that are actually views on content pages; vs all pages including non-content such as the home page, 
        channel/section landing pages, author pages, etc.
    '''
    # Sum pvs on actual content for user
    content_sum_pv=g[g.GA_cmsNaturalId.str.contains("blogandpostid|blogandslideid|galleryid|video")].GA_pageViews.sum()
    
    # Sum all pvs for user
    total_pv=g.GA_pageViews.sum()
    
    # Calculate ratio
    return (content_sum_pv) / total_pv


def get_top_n_pvs(df, category, n):
    top_list = df.groupby(category).sum().reset_index() \
        [[category, 'GA_pageViews']] \
        .sort_values(by='GA_pageViews', ascending=False).head(n) \
        [category].tolist()
    return top_list


def upload_file_gcs(bucket, file_blob, fn, local_file_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(file_blob)
    blob.upload_from_filename(local_file_path + fn)
    print('uploaded' + ' ' + str(blob))
    
    
def upload_bq(dataset_id, table_id, df, write_truncate=False):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    job_config.ignore_unknown_values = True
    job_config.allow_quoted_newlines = True
    if write_truncate:
        job_config.write_disposition = 'WRITE_TRUNCATE'

    try:
        job = client.load_table_from_dataframe(
            df,
            table_ref,
            location='US',
            job_config=job_config
        )
        job.result()
        print(f'{table_id} loaded into BQ')

    except Exception as e:
        print(e)

#upload to gcloud
def upload_file(fn, folder):
    file_blob = f'{folder}/{fn}'
    bucket = storage_client.bucket('bi-c-level')
    blob = bucket.blob(file_blob)
    blob.upload_from_filename(fn)
    print('uploaded' + ' ' + str(blob))
    