{{ config(
    materialized = 'incremental',
    unique_key = 'page_view_id',
    sort = 'tstamp',
    partition_by = {'field': 'tstamp', 'data_type': 'timestamp', 'granularity': var('segment_bigquery_partition_granularity')},
    dist = 'page_view_id',
    cluster_by = 'page_view_id'
    )}}

{#
the initial CTE in this model is unusually complicated; its function is to
select all pageviews (for all time) for users who have pageviews since the
model was most recently run. there are many window functions in this model so
in order to appropriately calculate all of them we need each users entire
page view history, but we only want to grab that for users who have page view
events we need to calculate.
#}

with pageviews as (

    select * from {{ref('segment_web_page_views')}}

    {% if is_incremental() %}
    where anonymous_id in (
        select distinct anonymous_id
        from {{ref('segment_web_page_views')}}

        {% if target.type == 'bigquery'%}
            where tstamp > (
            select 
                timestamp_sub(
                    max(tstamp), 
                    interval {{var('segment_sessionization_trailing_window')}} hour
                    )
            from {{ this }} )

        {% else %}
            where tstamp > (
            select
                {{ dbt_utils.dateadd(
                    'hour',
                    -var('segment_sessionization_trailing_window'),
                    'max(tstamp)'
                ) }}
            from {{ this }} )

        {% endif %}
    )
    {% endif %}

),

numbered as (

    --This CTE is responsible for assigning an all-time page view number for a
    --given anonymous_id. We don't need to do this across devices because the
    --whole point of this field is for sessionization, and sessions can't span
    --multiple devices.

    select

        *,

        row_number() over (
            partition by anonymous_id
            order by tstamp
            ) as page_view_number

    from pageviews

),

{% set fill_fields = [
    'utm_medium', 
    'utm_source', 
    'utm_campaign', 
    'gclid'] 
%}

fill_fields as (

    select

        *,
        
        {% for field in fill_fields %}
            first_value({{field}}) over (
                partition by anonymous_id, {{field}}
                order by tstamp
                rows between unbounded preceding and unbounded following
            ) as fill_{{field}} {% if not loop.last %} , {% endif %}
        {% endfor %}

    from numbered

),

lagged as (

    --This CTE is responsible for simply grabbing the last value of `tstamp`.
    --We'll use this downstream to do timestamp math--it's how we determine the
    --period of inactivity.

    select

        *,

        lag(tstamp) over (
            partition by anonymous_id
            order by page_view_number
        ) as previous_tstamp,

        {% for field in fill_fields %}
        lag(fill_{{field}}) over (
            partition by anonymous_id
            order by page_view_number
        ) as previous_fill_{{field}} {% if not loop.last %} , {% endif %}
        {% endfor %}

    from fill_fields

),

diffed as (

    --This CTE simply calculates `period_of_inactivity`.

    select
    
        *,

        {{ dbt_utils.datediff('previous_tstamp', 'tstamp', 'second') }} as period_of_inactivity,

        {% for field in fill_fields %}
        case
            when fill_{{field}} is null and previous_fill_{{field}} is null then false
            when fill_{{field}} is null and previous_fill_{{field}} is not null then false
            when fill_{{field}} is not null and previous_fill_{{field}} is null then true
            when fill_{{field}} != previous_fill_{{field}} then true
            else false
        end as is_new_{{field}} {% if not loop.last %} , {% endif %}
        {% endfor %}

    from lagged

),

new_sessions as (

    --This CTE calculates a single 1/0 field--if the period of inactivity prior
    --to this page view was greater than 30 minutes, the value is 1, otherwise
    --it's 0. We'll use this to calculate the user's session #.

    select

        *,

        case
            when period_of_inactivity > {{var('segment_inactivity_cutoff')}} then 1
            {% for field in fill_fields %}
            when is_new_{{field}} is true then 1
            {% endfor %}
            else 0
        end as new_session

    from diffed

),

session_numbers as (

    --This CTE calculates a user's session (1, 2, 3) number from `new_session`.
    --This single field is the entire point of the entire prior series of
    --calculations.

    select

        *,

        sum(new_session) over (
            partition by anonymous_id
            order by page_view_number
            rows between unbounded preceding and current row
            ) as session_number

    from new_sessions

),

session_ids as (

    --This CTE assigns a globally unique session id based on the combination of
    --`anonymous_id` and `session_number`.

    select

        {{dbt_utils.star(ref('segment_web_page_views'))}},
        page_view_number,
        {{dbt_utils.surrogate_key(['anonymous_id', 'session_number'])}} as session_id

    from session_numbers

)

select * from session_ids
