# SPDX-FileCopyrightText: Open Energy Transition gGmbH
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging
import sys
from pathlib import Path

import pandas as pd
import pypsa

PYPSA_EARTH_DIR = Path.cwd()
PYPSA_EARTH_SCRIPTS_DIR = PYPSA_EARTH_DIR / "scripts"
sys.path.append(str(PYPSA_EARTH_SCRIPTS_DIR))

from _helpers import sanitize_carriers, sanitize_locations
from process_cost_data import prepare_costs

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _add_ammonia_store(n, ammonia_buses, costs, store_suffix="ammonia store"):
    """
    Add extendable ammonia storage.
    """
    n.madd(
        "Store",
        ammonia_buses.index + f" {store_suffix}",
        bus=ammonia_buses.values,
        e_nom_extendable=True,
        e_cyclic=True,
        carrier=store_suffix,
        capital_cost=costs.at["NH3 (l) storage tank incl. liquefaction", "fixed"],
        lifetime=costs.at["NH3 (l) storage tank incl. liquefaction", "lifetime"],
    )


def add_grey_ammonia(n, industrial_demand, costs, config, nhours):
    """
    Add grey-ammonia explicit sector:
    gas -> grey H2 (SMR / SMR CC) -> grey-ammonia
    """
    if "grey_ammonia" not in industrial_demand.columns:
        logger.info("No grey_ammonia column found. Skipping grey-ammonia.")
        return

    nodes = industrial_demand.index
    grey_ammonia_bus = pd.Series(nodes + " grey-ammonia", index=nodes)
    grey_h2_bus = pd.Series(nodes + " grey H2", index=nodes)

    if "grey-ammonia" not in n.carriers.index:
        n.add("Carrier", "grey-ammonia")

    n.madd(
        "Bus",
        grey_ammonia_bus.values,
        location=nodes,
        carrier="grey-ammonia",
    )
    logger.info("Added grey-ammonia buses and carrier.")

    if "ammonia" in config.get("custom_industry", {}).get("production_flexibility", []):
        _add_ammonia_store(
            n,
            grey_ammonia_bus,
            costs,
            store_suffix="grey ammonia store",
        )
        logger.info("Added grey-ammonia stores.")

    n.madd(
        "Link",
        nodes + " grey Haber-Bosch",
        bus0=nodes,
        bus1=grey_ammonia_bus.values,
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

    p_set = (
        industrial_demand.loc[nodes, "grey_ammonia"].rename(
            index=lambda x: x + " grey-ammonia"
        )
        / nhours
    )

    n.madd(
        "Load",
        grey_ammonia_bus.values,
        bus=grey_ammonia_bus.values,
        p_set=p_set,
        carrier="grey-ammonia",
    )
    logger.info("Added grey-ammonia demand.")

    if "ammonia" in config.get("custom_industry", {}).get("ccs_retrofit", []):
        smr_links = n.links.query("carrier == 'SMR'").copy()

        if smr_links.empty:
            logger.warning("No SMR links found. Skipping ammonia CCS retrofit.")
            return

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
        logger.info("Added SMR CC for grey-ammonia retrofit.")


def add_e_ammonia(n, industrial_demand, costs, config, nhours):
    """
    Add e-ammonia explicit sector:
    electricity + grid H2 -> e-ammonia
    """
    if "e_ammonia" not in industrial_demand.columns:
        logger.info("No e_ammonia column found. Skipping e-ammonia.")
        return

    nodes = industrial_demand.index
    e_ammonia_bus = pd.Series(nodes + " e-ammonia", index=nodes)
    grid_h2_bus = pd.Series(nodes + " grid H2", index=nodes)

    if "e-ammonia" not in n.carriers.index:
        n.add("Carrier", "e-ammonia")

    n.madd(
        "Bus",
        e_ammonia_bus.values,
        location=nodes,
        carrier="e-ammonia",
    )
    logger.info("Added e-ammonia buses and carrier.")

    if "ammonia" in config.get("custom_industry", {}).get("production_flexibility", []):
        _add_ammonia_store(
            n,
            e_ammonia_bus,
            costs,
            store_suffix="e ammonia store",
        )
        logger.info("Added e-ammonia stores.")

    n.madd(
        "Link",
        nodes + " e Haber-Bosch",
        bus0=nodes,
        bus1=e_ammonia_bus.values,
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

    p_set = (
        industrial_demand.loc[nodes, "e_ammonia"].rename(
            index=lambda x: x + " e-ammonia"
        )
        / nhours
    )

    n.madd(
        "Load",
        e_ammonia_bus.values,
        bus=e_ammonia_bus.values,
        p_set=p_set,
        carrier="e-ammonia",
    )
    logger.info("Added e-ammonia demand.")


def add_grey_methanol(n, industrial_demand, costs, config, nhours):
    """
    Add grey-methanol explicit sector:
    gas + electricity -> grey-methanol + process CO2
    """
    if "grey_methanol" not in industrial_demand.columns:
        logger.info("No grey_methanol column found. Skipping grey-methanol.")
        return

    nodes = industrial_demand.index
    grey_methanol_bus = pd.Series(nodes + " grey-methanol", index=nodes)

    if "grey-methanol" not in n.carriers.index:
        n.add("Carrier", "grey-methanol")

    n.madd(
        "Bus",
        grey_methanol_bus.values,
        location=nodes,
        carrier="grey-methanol",
    )
    logger.info("Added grey-methanol buses and carrier.")

    n.madd(
        "Link",
        nodes + " grey methanol synthesis",
        bus0=nodes + " gas",
        bus1=grey_methanol_bus.values,
        bus2="co2 atmosphere",
        bus3=nodes,
        p_nom_extendable=True,
        carrier="grey methanol synthesis",
        efficiency=costs.at["grey methanol synthesis", "efficiency"],
        efficiency2=costs.at["grey methanol synthesis", "carbondioxide-output"],
        efficiency3=-costs.at["grey methanol synthesis", "electricity-input"],
        capital_cost=costs.at["grey methanol synthesis", "fixed"],
        marginal_cost=costs.at["grey methanol synthesis", "VOM"],
        lifetime=costs.at["grey methanol synthesis", "lifetime"],
    )
    logger.info("Added grey methanol synthesis links.")

    p_set = (
        industrial_demand.loc[nodes, "grey_methanol"].rename(
            index=lambda x: x + " grey-methanol"
        )
        / nhours
    )

    n.madd(
        "Load",
        grey_methanol_bus.values,
        bus=grey_methanol_bus.values,
        p_set=p_set,
        carrier="grey-methanol",
    )
    logger.info("Added grey-methanol demand.")


def add_e_methanol(n, industrial_demand, costs, config, nhours):
    """
    Add e-methanol explicit sector:
    electricity + grid H2 + co2 stored -> e-methanol
    """
    if "e_methanol" not in industrial_demand.columns:
        logger.info("No e_methanol column found. Skipping e-methanol.")
        return

    nodes = industrial_demand.index
    e_methanol_bus = pd.Series(nodes + " e-methanol", index=nodes)
    grid_h2_bus = pd.Series(nodes + " grid H2", index=nodes)
    co2_stored_bus = pd.Series(nodes + " co2 stored", index=nodes)

    if "e-methanol" not in n.carriers.index:
        n.add("Carrier", "e-methanol")

    n.madd(
        "Bus",
        e_methanol_bus.values,
        location=nodes,
        carrier="e-methanol",
    )
    logger.info("Added e-methanol buses and carrier.")

    n.madd(
        "Link",
        nodes + " methanolisation",
        bus0=nodes,
        bus1=e_methanol_bus.values,
        bus2=grid_h2_bus.values,
        bus3=co2_stored_bus.values,
        p_nom_extendable=True,
        carrier="e-methanol synthesis",
        efficiency=1 / costs.at["methanolisation", "electricity-input"],
        efficiency2=-costs.at["methanolisation", "hydrogen-input"]
        / costs.at["methanolisation", "electricity-input"],
        efficiency3=-costs.at["methanolisation", "carbondioxide-input"]
        / costs.at["methanolisation", "electricity-input"],
        capital_cost=costs.at["methanolisation", "fixed"]
        / costs.at["methanolisation", "electricity-input"],
        marginal_cost=costs.at["methanolisation", "VOM"]
        / costs.at["methanolisation", "electricity-input"],
        lifetime=costs.at["methanolisation", "lifetime"],
    )
    logger.info(
        "Added methanolisation links using electricity, grid H2 and co2 stored."
    )

    p_set = (
        industrial_demand.loc[nodes, "e_methanol"].rename(
            index=lambda x: x + " e-methanol"
        )
        / nhours
    )

    n.madd(
        "Load",
        e_methanol_bus.values,
        bus=e_methanol_bus.values,
        p_set=p_set,
        carrier="e-methanol",
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
    config = snakemake.config

    n = pypsa.Network(snakemake.input.network)

    industrial_demand = pd.read_csv(
        snakemake.input.industrial_energy_demand_per_node,
        index_col=0,
    )
    industrial_demand = industrial_demand.drop(columns=["country"], errors="ignore")

    nhours = n.snapshot_weightings.generators.sum()
    Nyears = nhours / 8760

    costs = prepare_costs(
        snakemake.input.costs,
        snakemake.config["costs"],
        snakemake.params.costs["output_currency"],
        snakemake.params.costs["fill_values"],
        Nyears,
        snakemake.params.costs["default_exchange_rate"],
        snakemake.params.costs["future_exchange_rate_strategy"],
        snakemake.params.costs["custom_future_exchange_rate"],
    )

    add_custom_explicit_industry(n, industrial_demand, costs, config, nhours)

    sanitize_carriers(n, snakemake.config)
    sanitize_locations(n)

    n.export_to_netcdf(snakemake.output.modified_network)
