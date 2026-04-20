{{ config(
    materialized='table',
    partition_by={
      "field": "release_year",
      "data_type": "string"
    },
    cluster_by=["fips_st_cnty"]
) }}

with staging as (

    select * from {{ ref('stg_ahrf') }}

),

fips_lookup as (

    select
        county_fips_code    as fips_st_cnty,
        area_name           as county_name,
        state_fips_code
        from {{ source('fips_lookup', 'fips_codes_all') }}
    where summary_level = '050'

),

final as (

    select
        -- Keys
        s.fips_st_cnty,
        f.county_name,
        f.state_fips_code,
        s.release_year,

        -- Primary care supply
        s.primary_care_mds,
        s.primary_care_dos,
        s.nurse_practitioners,
        s.physician_assistants,
        coalesce(s.primary_care_mds, 0) + coalesce(s.primary_care_dos, 0)
            + coalesce(s.nurse_practitioners, 0) + coalesce(s.physician_assistants, 0)
            as total_primary_care_providers,

        -- Safety net facilities
        s.fqhcs,
        s.community_health_centers,
        s.rural_health_clinics,
        s.critical_access_hospitals,

        -- Demand / need indicators
        s.population,
        s.population_65plus,
        s.pct_below_poverty,
        s.pct_uninsured_under65,
        s.unemployment_rate,
        s.median_household_income,

        -- Derived: providers per 10k population
        case
            when s.population > 0
            then round(
                (coalesce(s.primary_care_mds, 0) + coalesce(s.primary_care_dos, 0)
                + coalesce(s.nurse_practitioners, 0) + coalesce(s.physician_assistants, 0))
                / s.population * 10000, 2)
            else null
        end as primary_care_providers_per_10k,

        -- Other providers
        s.dentists_private_practice,
        s.psychiatrists,
        s.hospital_beds,
        s.short_term_gen_hospitals

    from staging s
left join fips_lookup f on LPAD(s.fips_st_cnty, 5, '0') = f.fips_st_cnty)

select * from final
