with staging as (

    select * from {{ ref('stg_ahrf') }}

),

final as (

    select
        -- Keys
        fips_st_cnty,
        release_year,

        -- Primary care supply
        primary_care_mds,
        primary_care_dos,
        nurse_practitioners,
        physician_assistants,
        coalesce(primary_care_mds, 0) + coalesce(primary_care_dos, 0)
            + coalesce(nurse_practitioners, 0) + coalesce(physician_assistants, 0)
            as total_primary_care_providers,

        -- Safety net facilities
        fqhcs,
        community_health_centers,
        rural_health_clinics,
        critical_access_hospitals,

        -- Demand / need indicators
        population,
        population_65plus,
        pct_below_poverty,
        pct_uninsured_under65,
        unemployment_rate,
        median_household_income,

        -- Derived: providers per 10k population
        case
            when population > 0
            then round(
                (coalesce(primary_care_mds, 0) + coalesce(primary_care_dos, 0)
                + coalesce(nurse_practitioners, 0) + coalesce(physician_assistants, 0))
                / population * 10000, 2)
            else null
        end as primary_care_providers_per_10k,

        -- Other providers
        dentists_private_practice,
        psychiatrists,
        hospital_beds,
        short_term_gen_hospitals

    from staging

)

select * from final
