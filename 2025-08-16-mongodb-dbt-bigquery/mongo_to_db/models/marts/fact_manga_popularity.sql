-- models/marts/fact_manga_popularity.sql

{{ config(materialized='table') }}

WITH manga_base AS (
    SELECT
        m.manga_id,
        m.title_en,
        m.year,
        m.status,
        m.demographic,
        m.content_rating,
        m.original_language,
        m.created_at AS manga_created_at,
        m.updated_at AS manga_updated_at
    FROM {{ ref('dim_manga') }} m
),

popularity_metrics AS (
    SELECT
        s.manga_id,
        AVG(s.rating_avg) AS avg_rating,
        AVG(s.rating_bayesian) AS avg_bayesian_rating,
        SUM(s.follows) AS total_follows,
        AVG(s.comments_replies_count) AS avg_comments_replies
    FROM {{ ref('fact_statistics') }} s
    GROUP BY s.manga_id
),

chapter_patterns AS (
    SELECT
        c.manga_id,
        COUNT(DISTINCT c.chapter_id) AS num_chapters,
        MIN(c.publish_at) AS first_chapter_publish_date,
        MAX(c.publish_at) AS last_chapter_publish_date,
        DATE_DIFF(MAX(c.publish_at), MIN(c.publish_at), DAY) 
        / COUNT(DISTINCT c.chapter_id) AS avg_days_between_chapters
    FROM {{ ref('fact_chapters') }} c
    GROUP BY c.manga_id
),

tags_aggregated AS (
    SELECT
        bt.manga_id,
        STRING_AGG(t.name_en, ', ') AS tags_list
    FROM {{ ref('bridge_manga_tag') }} bt
    LEFT JOIN {{ ref('dim_tag') }} t ON bt.tag_id = t.tag_id
    GROUP BY bt.manga_id
),

related_series AS (
    SELECT
        r.related_group_id AS manga_id,
        COUNT(DISTINCT r.entity_id) AS num_related_manga,
        STRING_AGG(r.relation_type, ', ') AS related_types  -- e.g., prequel, sequel
    FROM {{ ref('bridge_manga_related') }} r
    GROUP BY r.related_group_id
)

SELECT
    mb.manga_id,
    mb.title_en,
    mb.year,
    mb.status,
    mb.demographic,
    mb.content_rating,
    mb.original_language,
    mb.manga_created_at,
    mb.manga_updated_at,
    COALESCE(pm.avg_rating, 0) AS avg_rating,
    COALESCE(pm.avg_bayesian_rating, 0) AS avg_bayesian_rating,
    COALESCE(pm.total_follows, 0) AS total_follows,
    COALESCE(pm.avg_comments_replies, 0) AS avg_comments_replies,
    COALESCE(cp.num_chapters, 0) AS num_chapters,
    cp.first_chapter_publish_date,
    cp.last_chapter_publish_date,
    COALESCE(cp.avg_days_between_chapters, 0) AS avg_days_between_chapters,
    ta.tags_list,
    COALESCE(rs.num_related_manga, 0) AS num_related_manga,
    rs.related_types
FROM manga_base mb
LEFT JOIN popularity_metrics pm ON mb.manga_id = pm.manga_id
LEFT JOIN chapter_patterns cp ON mb.manga_id = cp.manga_id
LEFT JOIN tags_aggregated ta ON mb.manga_id = ta.manga_id
LEFT JOIN related_series rs ON mb.manga_id = rs.manga_id