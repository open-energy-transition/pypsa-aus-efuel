rule build_rooftop_solar_existing:
    input:
        cer="data/electricity/sgu-solar-capacity-2011-to-present-and-totals.csv",
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
        "scripts/custom_build_solar_rooftop_existing.py"
