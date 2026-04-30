# SPDX-FileCopyrightText: Open Energy Transition gGmbH
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging

import pandas as pd
import pypsa

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

H2_MWH_PER_TON = 33


def add_custom_hydrogen_demand(n, config, nhours):
    """
    Add aggregated custom hydrogen demand.

    The demand is represented by one national H2 market bus. Existing grid H2
    buses can supply this market through zero-cost extendable virtual links.
    This allows the optimiser to choose where hydrogen production is cheapest,
    while keeping final hydrogen use spatially aggregated.
    """
    h2_config = config.get("custom_hydrogen_demand", {})

    if not h2_config.get("enable", False):
        logger.info("Custom hydrogen demand disabled.")
        return n

    annual_demand_tpa = float(h2_config.get("annual_demand_tpa", 0.0))

    if annual_demand_tpa <= 0:
        logger.info("Custom hydrogen demand enabled but annual_demand_tpa is zero.")
        return n

    annual_demand_mwh = annual_demand_tpa * H2_MWH_PER_TON

    h2_buses = pd.Index(
        [
            bus
            for bus in n.buses.index
            if bus.endswith(" H2")
            and not any(
                bus.endswith(f" {color} H2")
                for color in ["grid", "green", "grey", "blue", "pink"]
            )
        ]
    )

    if h2_buses.empty:
        raise ValueError("No final '* H2' buses found for custom hydrogen demand.")

    market_bus = "custom H2 demand market"
    load_name = "custom H2 demand"

    if market_bus not in n.buses.index:
        n.add(
            "Bus",
            market_bus,
            carrier="H2",
        )

    if load_name in n.loads.index:
        raise ValueError(f"Load '{load_name}' already exists in the network.")

    n.add(
        "Load",
        load_name,
        bus=market_bus,
        carrier="H2",
        p_set=annual_demand_mwh / nhours,
    )

    link_names = h2_buses + " custom H2 demand supply"

    existing_links = pd.Index(link_names).intersection(n.links.index)
    if len(existing_links) > 0:
        raise ValueError(
            "Some custom H2 demand supply links already exist: "
            f"{list(existing_links)}"
        )

    n.madd(
        "Link",
        link_names,
        bus0=h2_buses,
        bus1=market_bus,
        carrier="custom H2 demand supply",
        p_nom_extendable=True,
        efficiency=1.0,
        capital_cost=0.0,
        marginal_cost=0.0,
    )

    logger.warning(
        f"Added custom aggregated H2 demand: {annual_demand_tpa:.2f} tH2/a "
        f"= {annual_demand_mwh:.2f} MWh/a, supplied by {len(h2_buses)} H2 buses."
    )

    return n


if __name__ == "__main__":
    config = snakemake.config

    n = pypsa.Network(snakemake.input.network)

    nhours = n.snapshot_weightings.generators.sum()

    n = add_custom_hydrogen_demand(n, config, nhours)

    n.export_to_netcdf(snakemake.output.modified_network)
