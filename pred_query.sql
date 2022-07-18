## content, GA and Sentiment
query = """
    WITH ga_data AS (
        SELECT
            GA_fullVisitorId,
            GA_visitStartTime, 
            GA_date,
            GA_pagePath,
            GA_dfpNewZone,
            GA_visitNumber,
            GA_pageViews,
            GA_scrollDepth,
            timeOnPage,  
            GA_cmsNaturalId,
            GA_deviceOperatingSystem,
            GA_deviceCategory,
            GA_deviceBrowser,
            GA_country,
            GA_referralGroup,
            GA_primaryChannel,
            GA_primarySection
        FROM
            `api-project-901373404215.DataMart.v_DataMart_updated`
        WHERE
            ga_date BETWEEN '2022-05-01' and '2022-05-31'
    ),
    
    sampled_fullvids AS (
        SELECT
            GA_fullVisitorId
        FROM (
            SELECT
                GA_fullVisitorId,
                RAND() as rand
            FROM
                `api-project-901373404215.DataMart.v_DataMart_updated`
            WHERE
                DATE(ga_date) BETWEEN '2022-05-01' and '2022-05-31'
        )
        ORDER BY rand
        LIMIT 100000
    ),
    sentiment as
    (
SELECT * EXCEPT (rank)
    FROM
    (
    SELECT 
          lower(natid) as sentiment_natid,
          pub_date, 
          clean_body, 
          sentiment_score,
          RANK() OVER (PARTITION BY natid ORDER BY pub_date DESC) AS rank
    FROM 
      `api-project-901373404215.sentiment.article_sentiment`
    )
    WHERE
           rank = 1 ),
    content_t AS (
        SELECT
            content_t.natid AS content_natid,
            publish_date,
            title,
            body,
            iab_cat_t.tier1
        FROM (
            SELECT 
                natid,
                EXTRACT(DATE FROM date_et) AS publish_date,
                title,
                body
            FROM (
                SELECT DISTINCT
                DATETIME(date, "America/New_York") as date_et,
                title,
                body,
                LOWER(NaturalId) AS natid,
                RANK() OVER (PARTITION BY naturalid ORDER BY timestamp DESC) AS mostrecent
            FROM
                `api-project-901373404215.Content.content`
            WHERE
                Visible is true
                    AND type in (
                    'blog',
                    'blogslide',
                    'magazine'
                )
            )
        WHERE 
            mostrecent = 1
        ) AS content_t
        INNER JOIN (
            SELECT
                * EXCEPT (rank)
            FROM (
                SELECT DISTINCT
                    category AS tier1,
                    natid,
                    RANK() OVER (PARTITION BY natid ORDER BY ts DESC) AS rank
                FROM
                    `api-project-901373404215.BusinessIntelligence.iab_cat_v2` 
            )
        WHERE
            rank = 1
        ) AS iab_cat_t
    ON 
        LOWER(iab_cat_t.natid) = LOWER(content_t.natid)
    ),
ga_content as 
    (
    SELECT
        GA_date,
        GA_fullVisitorId, 
        GA_visitStartTime,
        GA_visitNumber,
        GA_cmsNaturalId,
        publish_date,
        title,
        body,
        tier1,
        GA_pagePath,
        GA_dfpNewZone,
        timeOnPage,
        GA_scrollDepth,
        GA_pageViews,
        GA_deviceOperatingSystem,
        GA_deviceCategory,
        GA_deviceBrowser,
        GA_country,
        GA_referralGroup,
        GA_primaryChannel,
        GA_primarySection
    FROM
        ga_data
    
    LEFT OUTER JOIN
        content_t
    ON 
        LOWER(GA_cmsNaturalId) = LOWER(content_natid)
    WHERE
        GA_fullVisitorId IN (SELECT DISTINCT GA_fullVisitorId FROM sampled_fullvids)
)

SELECT 
    GA_date,
        GA_fullVisitorId, 
        GA_visitStartTime,
        GA_visitNumber,
        GA_cmsNaturalId,
        publish_date,
        title,
        body,
        clean_body,
        sentiment_score,
        tier1,
        GA_pagePath,
        GA_dfpNewZone,
        timeOnPage,
        GA_scrollDepth,
        GA_pageViews,
        GA_deviceOperatingSystem,
        GA_deviceCategory,
        GA_deviceBrowser,
        GA_country,
        GA_referralGroup,
        GA_primaryChannel,
        GA_primarySection
    FROM
        ga_content
    
    LEFT OUTER JOIN
        sentiment
    ON 
        LOWER(GA_cmsNaturalId) = LOWER(sentiment_natid)
    """

#subscriber data
query_sub= """
    WITH 
    active_subs AS (
        SELECT DISTINCT
                user_id_uid
        FROM
            `api-project-901373404215.piano.subscriber_details`
        WHERE
            # Filter for the 'universal' subscriptions only
            resource_id_rid IN UNNEST(['RKPEVDB', 'R8W03AS'])
            AND status='active'
            AND total__refunded<1
    ),
    -- Join with the datamart to get the ga fullvid
    sub_fullvids AS (
        SELECT DISTINCT
            ga_fullvisitorid
        FROM
            active_subs
        INNER JOIN
            `api-project-901373404215.DataMart.v_DataMart_updated`
        ON
            LOWER(ga_pianoId) = LOWER(user_id_uid)
    )
    -- Create a flag for subscribers based off the ga fullvid
    SELECT
        *
    FROM
        sub_fullvids
    """