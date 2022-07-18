import pandas as pd
import numpy as np
from utils import *


def get_user_feats(df):
    # Data cleansing

    # Drop NaNs
    df = df.dropna(subset=['GA_cmsNaturalId'])

    # Fill NaNs
    df['timeOnPage'] = df['timeOnPage'].fillna(0)
    
    df['sentiment_score'] = df['sentiment_score'].fillna(0)
    
    # Replace NULL and empty with "other" for tier1 categories
    df['tier1'] = df['tier1'].replace(r'^\s*$', 'other', regex=True)
    df['tier1'] = df['tier1'].fillna('other')
    
    # Use only the top n countries, channels and sections. 
    top_country_list = get_top_n_pvs(df, 'GA_country', 15)
    top_channel_list = get_top_n_pvs(df, 'GA_primaryChannel', 15)
    top_section_list = get_top_n_pvs(df, 'GA_primarySection', 15)
    
    # Give non-top categories a value of 'other'
    filter_cols = [
        'GA_country',
        'GA_primaryChannel',
        'GA_primarySection'
    ]

    shortlisted_cols = [
        top_country_list,
        top_channel_list,
        top_section_list
    ]

    for col, shortlisted_col in zip(filter_cols, shortlisted_cols):
        df[col] = np.where(
            df[col].isin(shortlisted_col),
            df[col],
            'other'
        )
    # User's per pagepath GA data for session and content views rate data
    page = df.groupby(['GA_fullVisitorId', 'GA_visitStartTime', 'GA_pagePath']) \
        .agg({'GA_pageViews': 'max', 'timeOnPage': 'sum'}).reset_index()
    
    # User's per session GA data
    session = page.groupby(['GA_fullVisitorId', 'GA_visitStartTime']) \
        .agg({'GA_pageViews': 'sum', 'timeOnPage': 'mean'}).reset_index() \
        .rename(columns={'GA_pageViews': 'session_pvs', 'timeOnPage': 'session_top'})
    
    # Mean/median of session PVs
    pvs = session.groupby('GA_fullVisitorId') \
        .agg({'session_pvs': ['mean', 'median']}).reset_index()

    # Rename cols
    pvs.columns = [' '.join(col).strip() for col in pvs.columns.values]
    pvs = pvs.rename(columns={'session_pvs mean':'session_pvs_mean', 'session_pvs median': 'session_pvs_median'})
    
    
    # Mean/Median time on page
    top = session.groupby('GA_fullVisitorId') \
        .agg({'session_top': ['mean', 'median']}).reset_index()

    # Rename cols
    top.columns = [' '.join(col).strip() for col in top.columns.values]
    top = top.rename(columns={'session_top mean':'session_top_mean', 'session_top median': 'session_top_median'})
    
    # Mean/Median Sentiment Score
    senti = df.groupby('GA_fullVisitorId') \
        .agg({'sentiment_score': ['mean', 'median']}).reset_index()

    # Rename cols
    senti.columns = [' '.join(col).strip() for col in senti.columns.values]
    senti = senti.rename(columns={'sentiment_score mean':'sentiment_score_mean', 'sentiment_score median': 'sentiment_score_median'})
    
    # PVs by referral source 
    ref = sum_pvs(
        df,
        'GA_referralGroup',
        prefix='rf_'
    )
    
    # PVs by country
    country = sum_pvs(
        df,
        'GA_country',
        prefix='ct_'
    )
    
    # PVs by device os
    device_os = sum_pvs(
        df,
        'GA_deviceOperatingSystem',
        prefix='dos_'
    )
    
    # PVs by tier 1 category
    t1_pvs = sum_pvs(
        df,
        'tier1',
        prefix='t1_'
    )
    
    # Perc of pageviews by t1 category
    t1_perc_pvs = t1_pvs.copy()

    calc_cols = [each for each in t1_perc_pvs.columns if each != 'GA_fullVisitorId']

    for col in calc_cols:
        t1_perc_pvs[f'{col}_perc_pvs'] = t1_perc_pvs[col] / t1_perc_pvs[calc_cols].sum(axis=1)

    t1_perc_pvs = t1_perc_pvs.drop(calc_cols, axis=1)
    
    # PVs by primary channel
    pc_pvs = sum_pvs(
        df,
        'GA_primaryChannel',
        prefix='pc_'
    )
    
    # PVs by primary section
    ps_pvs = sum_pvs(
        df,
        'GA_primarySection',
        prefix='ps_',
        suffix='_pvs'
    )
    
    # Avg Time on page for tier1
    t1_top = calc_top(
        df,
        'tier1',
        prefix='t1_',
        suffix='_top'
    )
    
    # Bounce rate
    br = pd.DataFrame(session.groupby('GA_fullVisitorId') \
        .apply(lambda x: b_rate(x))).reset_index() \
        .rename(columns={0:'bounce_rate'})
    
    # Content views rate
    natid_page_map = df[['GA_pagePath', 'GA_cmsNaturalId', 'publish_date']] \
        .sort_values('publish_date', ascending=False) \
        .drop_duplicates('GA_pagePath')

    # Join page path with their natids
    page = pd.merge(
        page,
        natid_page_map,
        how="left",
        on="GA_pagePath"
    )

    cvr = pd.DataFrame(page.groupby('GA_fullVisitorId') \
        .apply(lambda x: c_views_rate(x))).reset_index() \
        .rename(columns={0:'content_views_rate'})
    
    # Day based features. Day of the week, Weekpart, day of the month, month number
    df['GA_date'] = pd.to_datetime(df['GA_date'], errors='coerce')

    df['dayofweek'] = df.GA_date.dt.day_name()

    week_dict = {True: 'weekday', False: 'weekend'}
    df['weekday'] = ((df.GA_date.dt.dayofweek)// 5 != 1).astype('category')
    df['weekday'] = df['weekday'].map(week_dict)

    df['day'] = df['GA_date'].dt.day
    df['month'] = df['GA_date'].dt.month
    
    # Time on page by weekpart
    weekdays = df[['GA_fullVisitorId', 'weekday', 'GA_pageViews']]

    day = weekdays[weekdays['weekday'] == 'weekday'].groupby('GA_fullVisitorId')['GA_pageViews'].sum().reset_index()
    day.columns = ['GA_fullVisitorId','weekday_pvs']

    end = weekdays[weekdays['weekday'] == 'weekend'].groupby('GA_fullVisitorId')['GA_pageViews'].sum().reset_index()
    end.columns = ['GA_fullVisitorId','weekend_pvs']

    wk_df = day.merge(end, how='outer')
    wk_df = wk_df.fillna(0)

    wk_top = calc_top(
        df,
        'weekday',
        suffix='_top'
    )
    
    # Time based features. Session time, est time session time, hour, minute, business hours
    df['session_time'] = df['GA_visitStartTime'].apply(convert_time)
    df['session_time'] = pd.to_datetime(df['session_time'], errors='coerce')
    df['est_time'] = df['session_time'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

    # for US, calculate hour from EST conversion. Else calculate hour from GMT
    df['hour'] = np.where(
        (df['GA_country']=='united states'),
        df.est_time.dt.hour,
        df.session_time.dt.hour
    )

    df['minute'] = np.where(
        (df['GA_country']=="united states"),
        df['est_time'].dt.minute,
        df['session_time'].dt.minute
    )

    df['business_hours'] = np.where(
        (df['hour']>=8) & (df['hour']<18),
        'business_hours',
        'non_business_hours'
    )
    
    # Pageviews by business/non-business hours
    busi_df = sum_pvs(
        df,
        'business_hours',
        suffix='_pvs'
    )
    
    # Avg time on page by business/non-business hours
    busi_top = calc_top(
        df,
        'business_hours',
        suffix='_top'
    )
    
    # Avg time on page by day of the week
    dow_top = calc_top(
        df,
        'dayofweek',
        suffix='_top'
    )

    # Avg time on page by day of the month
    dom_top = calc_top(
        df,
        'day',
        prefix='day_of_mon_',
        suffix='_top'
    )
    
    
    # Merge all the transformed dfs together
    df_list = [
        pvs,
        top,
        senti,
        ref,
        country,
        device_os,
        t1_pvs,
        t1_perc_pvs,
        pc_pvs,
        ps_pvs,
        t1_top,
        br,
        cvr,
        wk_top,
        busi_top,
        dow_top,
        dom_top
    ]

    #merge
    df = multiple_df_merge(
        df_list = df_list,
        on='GA_fullVisitorId',
        how='inner'
    ).fillna(0)

    return df
    
