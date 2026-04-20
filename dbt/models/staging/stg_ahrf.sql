with source as (

    select * from {{ source('ahrf_raw', 'ahrf_2022_2023') }}
    union all
    select * from {{ source('ahrf_raw', 'ahrf_2023_2024') }}
    union all
    select * from {{ source('ahrf_raw', 'ahrf_2024_2025') }}

),

renamed as (

    select
        -- GEO keys
        cast(fips_st_cnty as string)                as fips_st_cnty,

        -- Healthcare providers
        cast(primary_care_mds as numeric)           as primary_care_mds,
        cast(primary_care_dos as numeric)           as primary_care_dos,
        cast(nurse_practitioners as numeric)        as nurse_practitioners,
        cast(physician_assistants as numeric)       as physician_assistants,
        cast(dentists_private_practice as numeric)  as dentists_private_practice,
        cast(psychiatrists as numeric)              as psychiatrists,

        -- Healthcare facilities
        cast(fqhcs as numeric)                      as fqhcs,
        cast(community_health_centers as numeric)   as community_health_centers,
        cast(short_term_gen_hospitals as numeric)   as short_term_gen_hospitals,
        cast(hospital_beds as numeric)              as hospital_beds,
        cast(critical_access_hospitals as numeric)  as critical_access_hospitals,
        cast(rural_health_clinics as numeric)       as rural_health_clinics,

        -- Population / SDOH
        cast(population as numeric)                 as population,
        cast(population_65plus as numeric)          as population_65plus,
        cast(pct_below_poverty as numeric)          as pct_below_poverty,
        cast(pct_uninsured_under65 as numeric)      as pct_uninsured_under65,
        cast(median_household_income as numeric)    as median_household_income,
        cast(unemployment_rate as numeric)          as unemployment_rate,

        -- Metadata
        cast(release_year as string)                as release_year

    from source

)

select * from renamed
