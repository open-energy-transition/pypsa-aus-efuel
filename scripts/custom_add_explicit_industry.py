# SPDX-FileCopyrightText: Open Energy Transition gGmbH
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import os
import sys

import pandas as pd

sys.path.append(os.path.abspath(os.path.join(__file__, "../../")))
sys.path.append(
    os.path.abspath(os.path.join(__file__, "../../submodules/pypsa-earth/scripts/"))
)

from _helpers import prepare_costs

from scripts._helper import (
    configure_logging,
    create_logger,
    load_network,
    mock_snakemake,
    update_config_from_wildcards,
)

logger = create_logger(__name__)


def _add_nh3_store(n, nh3_buses, costs, store_suffix="ammonia store"):
    """
    Add extendable ammonia storage.
    """
    n.madd(
        "Store",
        nh3_buses.index + f" {store_suffix}",
        bus=nh3_buses.values,
        e_nom_extendable=True,
        e_cyclic=True,
        carrier=store_suffix,
        capital_cost=costs.at["NH3 (l) storage tank incl. liquefaction", "fixed"],
        lifetime=costs.at["NH3 (l) storage tank incl. liquefaction", "lifetime"],
    )


def add_grey_ammonia(n, industrial_demand, costs, config, nhours):
    """
    Add grey ammonia explicit sector:
    gas -> grey H2 (SMR / SMR CC) -> grey NH3
    """
    if "grey_ammonia" not in industrial_demand.columns:
        logger.info("No grey_ammonia column found. Skipping grey ammonia.")
        return

    nodes = industrial_demand.index
    grey_nh3_bus = pd.Series(nodes + " grey NH3", index=nodes)
    grey_h2_bus = pd.Series(nodes + " grey H2", index=nodes)

    # Carrier
    if "grey NH3" not in n.carriers.index:
        n.add("Carrier", "grey NH3")

    # NH3 buses
    n.madd(
        "Bus",
        grey_nh3_bus.values,
        location=nodes,
        carrier="grey NH3",
    )
    logger.info("Added grey ammonia buses and carrier.")

    # Optional production flexibility
    if "production_flexibility" in config.get("custom_industry", {}):
        if "ammonia" in config["custom_industry"]["production_flexibility"]:
            _add_nh3_store(n, grey_nh3_bus, costs, store_suffix="grey ammonia store")
            logger.info("Added grey ammonia stores.")

    # Grey Haber-Bosch: electricity + grey H2 -> grey NH3
    n.madd(
        "Link",
        nodes + " grey Haber-Bosch",
        bus0=nodes,
        bus1=grey_nh3_bus.values,
        bus2=grey_h2_bus.values,
        p_nom_extendable=True,
        carrier="grey Haber-Bosch",
        efficiency=1 / costs.at["Haber-Bosch", "electricity-input"],
        efficiency2=-costs.at["Haber-Bosch", "hydrogen-input"]
        / costs.at["Haber-Bosch", "electricity-input"],
        capital_cost=costs.at["Haber-Bosch", "fixed"]
        / costs.at["Haber-Bosch", "electricity-input"],
        marginal_cost=costs.at["Haber-Bosch", "VOM"]
        / costs.at["Haber-Bosch", "electricity-input"],
        lifetime=costs.at["Haber-Bosch", "lifetime"],
    )
    logger.info("Added grey Haber-Bosch process.")

    # Grey ammonia demand
    p_set = (
        industrial_demand.loc[nodes, "grey_ammonia"].rename(
            index=lambda x: x + " grey NH3"
        )
        / nhours
    )

    n.madd(
        "Load",
        grey_nh3_bus.values,
        bus=grey_nh3_bus.values,
        p_set=p_set,
        carrier="grey NH3",
    )
    logger.info("Added grey ammonia demand.")

    # CCS retrofit for grey ammonia: retrofit SMR -> SMR CC on grey H2 buses
    if "ammonia" in config["custom_industry"]["ccs_retrofit"]:
        smr_links = n.links.query("carrier == 'SMR'").copy()

        if smr_links.empty:
            logger.warning("No SMR links found. Skipping ammonia CCS retrofit.")
            return

        # only keep SMR links producing grey H2 when hydrogen_colors is enabled
        smr_links = smr_links[smr_links["bus1"].str.endswith(" grey H2")]

        if smr_links.empty:
            logger.warning(
                "No SMR links connected to grey H2 buses found. Skipping ammonia CCS retrofit."
            )
            return

        smr_cc_index = smr_links.index + " CC"
        gas_buses = smr_links.bus0
        h2_buses = smr_links.bus1
        co2_stored_buses = gas_buses.str.replace("gas", "co2 stored")
        elec_buses = gas_buses.str.replace(" gas", "")

        capital_cost = (
            costs.at["SMR", "fixed"]
            + costs.at["ammonia carbon capture retrofit", "fixed"]
            * costs.at["gas", "CO2 intensity"]
            * costs.at["ammonia carbon capture retrofit", "capture_rate"]
        )

        n.madd(
            "Link",
            smr_cc_index,
            bus0=gas_buses.values,
            bus1=h2_buses.values,
            bus2="co2 atmosphere",
            bus3=co2_stored_buses.values,
            bus4=elec_buses.values,
            p_nom_extendable=True,
            carrier="SMR CC",
            efficiency=costs.at["SMR CC", "efficiency"],
            efficiency2=costs.at["gas", "CO2 intensity"]
            * (1 - costs.at["ammonia carbon capture retrofit", "capture_rate"]),
            efficiency3=costs.at["gas", "CO2 intensity"]
            * costs.at["ammonia carbon capture retrofit", "capture_rate"],
            efficiency4=-costs.at[
                "ammonia carbon capture retrofit", "electricity-input"
            ]
            * costs.at["gas", "CO2 intensity"]
            * costs.at["ammonia carbon capture retrofit", "capture_rate"],
            capital_cost=capital_cost,
            lifetime=costs.at["ammonia carbon capture retrofit", "lifetime"],
        )
        logger.info("Added SMR CC for grey ammonia retrofit.")


def add_e_ammonia(n, industrial_demand, costs, config, nhours):
    """
    Add e-ammonia explicit sector:
    electricity + grid H2 -> e NH3
    """
    if "e_ammonia" not in industrial_demand.columns:
        logger.info("No e_ammonia column found. Skipping e-ammonia.")
        return

    nodes = industrial_demand.index
    e_nh3_bus = pd.Series(nodes + " e NH3", index=nodes)
    grid_h2_bus = pd.Series(nodes + " grid H2", index=nodes)

    # Carrier
    if "e NH3" not in n.carriers.index:
        n.add("Carrier", "e NH3")

    # NH3 buses
    n.madd(
        "Bus",
        e_nh3_bus.values,
        location=nodes,
        carrier="e NH3",
    )
    logger.info("Added e-ammonia buses and carrier.")

    # Optional production flexibility
    if "production_flexibility" in config.get("custom_industry", {}):
        if "ammonia" in config["custom_industry"]["production_flexibility"]:
            _add_nh3_store(n, e_nh3_bus, costs, store_suffix="e ammonia store")
            logger.info("Added e-ammonia stores.")

    # e-Haber-Bosch: electricity + grid H2 -> e NH3
    n.madd(
        "Link",
        nodes + " e Haber-Bosch",
        bus0=nodes,
        bus1=e_nh3_bus.values,
        bus2=grid_h2_bus.values,
        p_nom_extendable=True,
        carrier="e Haber-Bosch",
        efficiency=1 / costs.at["Haber-Bosch", "electricity-input"],
        efficiency2=-costs.at["Haber-Bosch", "hydrogen-input"]
        / costs.at["Haber-Bosch", "electricity-input"],
        capital_cost=costs.at["Haber-Bosch", "fixed"]
        / costs.at["Haber-Bosch", "electricity-input"],
        marginal_cost=costs.at["Haber-Bosch", "VOM"]
        / costs.at["Haber-Bosch", "electricity-input"],
        lifetime=costs.at["Haber-Bosch", "lifetime"],
    )
    logger.info("Added e-Haber-Bosch process using grid H2.")

    # e-ammonia demand
    p_set = (
        industrial_demand.loc[nodes, "e_ammonia"].rename(index=lambda x: x + " e NH3")
        / nhours
    )

    n.madd(
        "Load",
        e_nh3_bus.values,
        bus=e_nh3_bus.values,
        p_set=p_set,
        carrier="e NH3",
    )
    logger.info("Added e-ammonia demand.")


def _add_methanol_store(n, methanol_buses, costs, store_suffix="methanol store"):
    """
    Add extendable methanol storage.
    """
    n.madd(
        "Store",
        methanol_buses.index + f" {store_suffix}",
        bus=methanol_buses.values,
        e_nom_extendable=True,
        e_cyclic=True,
        carrier=store_suffix,
        # TODO: replace with dedicated methanol storage cost when available
        capital_cost=0,
        lifetime=25,
    )


def add_grey_methanol(n, industrial_demand, costs, config, nhours):
    """
    Add grey methanol explicit sector:
    gas -> grey methanol
    """
    if "grey_methanol" not in industrial_demand.columns:
        logger.info("No grey_methanol column found. Skipping grey methanol.")
        return

    nodes = industrial_demand.index
    grey_methanol_bus = pd.Series(nodes + " grey methanol", index=nodes)

    if "grey methanol" not in n.carriers.index:
        n.add("Carrier", "grey methanol")

    n.madd(
        "Bus",
        grey_methanol_bus.values,
        location=nodes,
        carrier="grey methanol",
    )
    logger.info("Added grey methanol buses and carrier.")

    if "production_flexibility" in config.get("custom_industry", {}):
        if "methanol" in config["custom_industry"]["production_flexibility"]:
            _add_methanol_store(
                n,
                grey_methanol_bus,
                costs,
                store_suffix="grey methanol store",
            )
            logger.info("Added grey methanol stores.")

    # Grey methanol: gas -> methanol
    # TODO: replace placeholders with dedicated methanol techno-economic data
    n.madd(
        "Link",
        nodes + " grey methanol synthesis",
        bus0=nodes + " gas",
        bus1=grey_methanol_bus.values,
        p_nom_extendable=True,
        carrier="grey methanol synthesis",
        efficiency=1.0,
        capital_cost=0,
        marginal_cost=0,
        lifetime=25,
    )
    logger.info("Added grey methanol synthesis links.")

    p_set = (
        industrial_demand.loc[nodes, "grey_methanol"].rename(
            index=lambda x: x + " grey methanol"
        )
        / nhours
    )

    n.madd(
        "Load",
        grey_methanol_bus.values,
        bus=grey_methanol_bus.values,
        p_set=p_set,
        carrier="grey methanol",
    )
    logger.info("Added grey methanol demand.")


def add_e_methanol(n, industrial_demand, costs, config, nhours):
    """
    Add e-methanol explicit sector:
    grid H2 + co2 stored -> e methanol
    """
    if "e_methanol" not in industrial_demand.columns:
        logger.info("No e_methanol column found. Skipping e-methanol.")
        return

    nodes = industrial_demand.index
    e_methanol_bus = pd.Series(nodes + " e methanol", index=nodes)
    grid_h2_bus = pd.Series(nodes + " grid H2", index=nodes)
    co2_stored_bus = pd.Series(nodes + " co2 stored", index=nodes)

    if "e methanol" not in n.carriers.index:
        n.add("Carrier", "e methanol")

    n.madd(
        "Bus",
        e_methanol_bus.values,
        location=nodes,
        carrier="e methanol",
    )
    logger.info("Added e-methanol buses and carrier.")

    if "production_flexibility" in config.get("custom_industry", {}):
        if "methanol" in config["custom_industry"]["production_flexibility"]:
            _add_methanol_store(
                n,
                e_methanol_bus,
                costs,
                store_suffix="e methanol store",
            )
            logger.info("Added e-methanol stores.")

    # e-methanol: grid H2 + co2 stored -> methanol
    # TODO: replace placeholders with dedicated e-methanol techno-economic data
    n.madd(
        "Link",
        nodes + " e methanol synthesis",
        bus0=grid_h2_bus.values,
        bus1=e_methanol_bus.values,
        bus2=co2_stored_bus.values,
        p_nom_extendable=True,
        carrier="e methanol synthesis",
        efficiency=1.0,
        efficiency2=-1.0,
        capital_cost=0,
        marginal_cost=0,
        lifetime=25,
    )
    logger.info("Added e-methanol synthesis links using grid H2 and co2 stored.")

    p_set = (
        industrial_demand.loc[nodes, "e_methanol"].rename(
            index=lambda x: x + " e methanol"
        )
        / nhours
    )

    n.madd(
        "Load",
        e_methanol_bus.values,
        bus=e_methanol_bus.values,
        p_set=p_set,
        carrier="e methanol",
    )
    logger.info("Added e-methanol demand.")


def add_custom_explicit_industry(n, industrial_demand, costs, config, nhours):
    """
    Add all custom explicit industry sectors currently implemented.
    """
    add_grey_ammonia(n, industrial_demand, costs, config, nhours)
    add_e_ammonia(n, industrial_demand, costs, config, nhours)
    add_grey_methanol(n, industrial_demand, costs, config, nhours)
    add_e_methanol(n, industrial_demand, costs, config, nhours)
    return n


if __name__ == "__main__":
    if "snakemake" not in globals():
        snakemake = mock_snakemake(
            "custom_add_explicit_industry",
            simpl="",
            ll="v1",
            clusters=10,
            opts="Co2L-3h",
            sopts="3h",
            planning_horizons="2030",
            discountrate="0.071",
            demand="AB",
            configfile="config.yaml",
        )

    configure_logging(snakemake)

    config = update_config_from_wildcards(snakemake.config, snakemake.wildcards)

    n = load_network(snakemake.input.network)

    industrial_demand = pd.read_csv(
        snakemake.input.industrial_energy_demand_per_node,
        index_col=0,
    )

    nhours = n.snapshot_weightings.generators.sum()
    Nyears = nhours / 8760

    costs = prepare_costs(
        snakemake.input.costs,
        snakemake.config["costs"],
        snakemake.params.costs["output_currency"],
        snakemake.params.costs["fill_values"],
        Nyears,
        snakemake.params.costs["default_exchange_rate"],
        reference_year=snakemake.config["costs"].get("reference_year", 2020),
    )

    add_custom_explicit_industry(n, industrial_demand, costs, config, nhours)

    n.export_to_netcdf(snakemake.output.modified_network)
