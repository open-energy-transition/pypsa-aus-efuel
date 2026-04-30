rule build_rooftop_solar_existing:
    input:
        cer="../data/electricity/sgu-solar-capacity-2011-to-present-and-totals.csv",
        poa_shp="../data/shapes/POA_2021_AUST_GDA2020_SHP/POA_2021_AUST_GDA2020.shp",
        network="networks/" + RDIR + "elec_s{simpl}_{clusters}.nc",
    output:
        "resources/"
        + RDIR
        + "rooftop_solar_existing_elec_s{simpl}_{clusters}_{planning_horizons}.csv",
    log:
        "logs/"
        + RDIR
        + "build_rooftop_solar_existing_elec_s{simpl}_{clusters}_{planning_horizons}.log",
    benchmark:
        "benchmarks/"
        + RDIR
        + "build_rooftop_solar_existing_elec_s{simpl}_{clusters}_{planning_horizons}"
    threads: 1
    resources:
        mem_mb=4000
    script:
        "../scripts/custom_build_solar_rooftop_existing.py"

if config.get("custom_industry", {}).get("enable", False):

    rule build_custom_industry_demand:
        params:
            countries=config["countries"],
            gadm_layer_id=config["build_shape_options"]["gadm_layer_id"],
            alternative_clustering=config["cluster_options"]["alternative_clustering"],
            demand_allocation_mode=lambda wildcards: config["custom_industry"]["demand_allocation"]["mode"],
            targets_tpa=lambda wildcards: str(config["custom_industry"]["targets_tpa"]),
            e_share=lambda wildcards: str(config["custom_industry"]["e_share"]),
        input:
            gem_data="../data/industry/Plant-level-data-Global-Chemicals-Inventory-November-2025-V1.xlsx",
            capacity_data="../data/industry/ammonia_methanol_production_per_plant_au.xlsx",
            shapes_path="resources/" + RDIR + "bus_regions/regions_onshore_elec_s{simpl}_{clusters}.geojson",
        output:
            industrial_energy_demand_per_node=(
                "resources/"
                + SECDIR
                + "demand/industrial_energy_demand_per_node_elec_s{simpl}_{clusters}_{planning_horizons}_{demand}_custom_industry.csv"
            ),
            plants=(
                "resources/"
                + SECDIR
                + "demand/custom_industry_plants_elec_s{simpl}_{clusters}_{planning_horizons}_{demand}.csv"
            ),
            growth_targets=(
                "resources/"
                + SECDIR
                + "demand/custom_industry_growth_targets_elec_s{simpl}_{clusters}_{planning_horizons}_{demand}.csv"
            ),
        log:
            "logs/"
            + SECDIR
            + "build_custom_industry_demand/elec_s{simpl}_{clusters}_{planning_horizons}_{demand}.log",
        benchmark:
            "benchmarks/"
            + SECDIR
            + "build_custom_industry_demand/elec_s{simpl}_{clusters}_{planning_horizons}_{demand}",
        threads: 1
        resources:
            mem_mb=4000,
        script:
            "../scripts/custom_build_industry_demand.py"


if config.get("custom_industry", {}).get("enable", False):

    rule add_custom_explicit_industry:
        params:
            costs=config["costs"],
        input:
            industrial_energy_demand_per_node=(
                "resources/"
                + SECDIR
                + "demand/industrial_energy_demand_per_node_elec_s{simpl}_{clusters}_{planning_horizons}_{demand}_custom_industry.csv"
            ),
            network=(
                "results/"
                + SECDIR
                + "prenetworks/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_{sopts}_{planning_horizons}_{discountrate}_{demand}.nc"
            ),
            costs="resources/" + RDIR + "costs_{planning_horizons}.csv",
            growth_targets=(
                "resources/"
                + SECDIR
                + "demand/custom_industry_growth_targets_elec_s{simpl}_{clusters}_{planning_horizons}_{demand}.csv"
            ),
        output:
            modified_network=(
                "results/"
                + SECDIR
                + "prenetworks/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_{sopts}_{planning_horizons}_{discountrate}_{demand}_custom_industry.nc"
            ),
        log:
            "logs/"
            + SECDIR
            + "add_custom_explicit_industry/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_{sopts}_{planning_horizons}_{discountrate}_{demand}.log",
        benchmark:
            "benchmarks/"
            + SECDIR
            + "add_custom_explicit_industry/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_{sopts}_{planning_horizons}_{discountrate}_{demand}",
        threads: 1
        resources:
            mem_mb=4000,
        script:
            "../scripts/custom_add_explicit_industry.py"


if config.get("custom_hydrogen_demand", {}).get("enable", False):

    rule add_custom_hydrogen_demand:
        params:
            annual_demand_tpa=lambda wildcards: config["custom_hydrogen_demand"]["annual_demand_tpa"],
        input:
            network=(
                "results/"
                + SECDIR
                + "prenetworks/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_{sopts}_{planning_horizons}_{discountrate}_{demand}_custom_industry.nc"
            ),
        output:
            modified_network=(
                "results/"
                + SECDIR
                + "prenetworks/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_{sopts}_{planning_horizons}_{discountrate}_{demand}_custom_hydrogen_demand.nc"
            ),
        log:
            "logs/"
            + SECDIR
            + "add_custom_hydrogen_demand/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_{sopts}_{planning_horizons}_{discountrate}_{demand}.log",
        benchmark:
            "benchmarks/"
            + SECDIR
            + "add_custom_hydrogen_demand/elec_s{simpl}_{clusters}_ec_l{ll}_{opts}_{sopts}_{planning_horizons}_{discountrate}_{demand}",
        threads: 1
        resources:
            mem_mb=4000,
        script:
            "../scripts/custom_add_hydrogen_demand.py"
