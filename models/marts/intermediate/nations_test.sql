with nations as (
    select * from {{ ref('stg_tpch_nations') }}
),

nations_ab as (
    select * from {{ ref('nations_csv') }}
)


select
nations.name,
nations.nation_key,
nations.comment,
nations_ab.nation_abbrev
from nations
left join nations_ab on nations.nation_key = nations_ab.nation_key


