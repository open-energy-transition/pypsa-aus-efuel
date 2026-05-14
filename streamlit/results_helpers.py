# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pypsa


def get_snapshot_weightings(n: pypsa.Network) -> pd.Series:
    """Return the best available snapshot weighting series."""
    if "generators" in n.snapshot_weightings.columns:
        return n.snapshot_weightings.generators

    if "objective" in n.snapshot_weightings.columns:
        return n.snapshot_weightings.objective

    return pd.Series(1.0, index=n.snapshots)


def get_available_result_categories() -> list[str]:
    """Return result categories exposed in the Streamlit results explorer."""
    return [
        "Electricity",
        "Hydrogen",
        "Ammonia / e-ammonia",
        "Methanol / e-methanol",
        "CO2 capture",
    ]


def rename_carrier(carrier: str) -> str:
    """Return display name for carrier."""
    mapping = {
        "solar": "Utility solar",
        "solar rooftop": "Rooftop solar",
        "onwind": "Onshore wind",
        "offwind-ac": "Offshore wind AC",
        "offwind-dc": "Offshore wind DC",
        "ror": "Run-of-river hydro",
        "PHS": "Pumped hydro",
        "CCGT": "Gas CCGT",
        "OCGT": "Gas OCGT",
        "coal": "Coal",
        "oil": "Oil",
        "hydro": "Hydro",
        "biomass": "Biomass",
        "battery discharger": "Battery",
        "Alkaline electrolyzer large": "Alkaline electrolyzer large",
        "Alkaline electrolyzer medium": "Alkaline electrolyzer medium",
        "Alkaline electrolyzer small": "Alkaline electrolyzer small",
        "PEM electrolyzer": "PEM electrolyzer",
        "SOEC": "SOEC",
        "SMR": "SMR",
        "Solid biomass steam reforming": "Biomass steam reforming",
        "Biomass gasification": "Biomass gasification",
        "Biomass gasification CC": "Biomass gasification + CCS",
        "Natural gas steam reforming": "Natural gas steam reforming",
        "Natural gas steam reforming CC": "Natural gas steam reforming + CCS",
        "Coal gasification": "Coal gasification",
        "Coal gasification CC": "Coal gasification + CCS",
        "Heavy oil partial oxidation": "Heavy oil partial oxidation",
        "grey Haber-Bosch": "Grey ammonia",
        "e Haber-Bosch": "e-ammonia",
        "grey methanol synthesis": "Grey methanol",
        "e-methanol synthesis": "e-methanol",
        "SMR CC": "SMR CC",
    }
    return mapping.get(carrier, carrier)


def get_category_carriers(category: str) -> dict[str, list[str]]:
    """Return component-specific exact carriers for each result category."""
    mapping = {
        "Electricity": {
            "generators": [
                "solar",
                "solar rooftop",
                "onwind",
                "offwind-ac",
                "offwind-dc",
                "ror",
                "biomass",
                "coal",
                "oil",
            ],
            "links": [
                "OCGT",
                "CCGT",
                "coal",
                "oil",
                "biomass",
                "battery discharger",
            ],
            "storage_units": [
                "PHS",
                "hydro",
            ],
            "stores": [
                "battery",
            ],
            "loads": [
                "AC",
                "industry electricity",
            ],
        },
        "Hydrogen": {
            "links": [
                "grid H2",
                "grey H2",
                "blue H2",
                "H2 Fuel Cell",
                "H2 pipeline",
                "H2 pipeline repurposed",
                "H2 Electrolysis",
            ],
            "stores": [
                "H2",
                "H2 Store Tank",
            ],
            "loads": [
                "H2",
            ],
            "buses": [
                "H2",
                "grid H2",
                "grey H2",
                "blue H2",
            ],
        },
        "Ammonia / e-ammonia": {
            "links": [
                "grey Haber-Bosch",
                "e Haber-Bosch",
            ],
            "loads": [
                "grey-ammonia",
                "e-ammonia",
            ],
            "buses": [
                "grey-ammonia",
                "e-ammonia",
            ],
        },
        "Methanol / e-methanol": {
            "links": [
                "grey methanol synthesis",
                "e-methanol synthesis",
            ],
            "loads": [
                "grey-methanol",
                "e-methanol",
            ],
            "buses": [
                "grey-methanol",
                "e-methanol",
            ],
        },
        "CO2 capture": {
            "links": [
                "SMR CC",
            ],
            "buses": [
                "co2 stored",
            ],
        },
    }

    return mapping.get(category, {})


def compute_capacity_by_carrier(
    networks: dict[str, pypsa.Network],
    category: str,
) -> pd.DataFrame:
    """Compute optimized capacity by exact carrier for selected scenarios."""
    category_carriers = get_category_carriers(category)
    rows = []

    for scenario, n in networks.items():
        generator_carriers = category_carriers.get("generators", [])
        if (
            generator_carriers
            and not n.generators.empty
            and "p_nom_opt" in n.generators.columns
        ):
            df = n.generators[n.generators["carrier"].isin(generator_carriers)]
            df = df.copy()
            df["capacity_gw"] = df["p_nom_opt"] * df["efficiency"].fillna(1.0) / 1e3

            for carrier, value in df.groupby("carrier")["capacity_gw"].sum().items():
                rows.append(
                    {
                        "scenario": scenario,
                        "component": "Generator",
                        "carrier": rename_carrier(carrier),
                        "value": value,
                        "unit": "GW",
                    }
                )

        link_carriers = category_carriers.get("links", [])
        if link_carriers and not n.links.empty and "p_nom_opt" in n.links.columns:
            df = n.links[n.links["carrier"].isin(link_carriers)].copy()
            df["capacity_gw"] = df["p_nom_opt"] * df["efficiency"].fillna(1.0) / 1e3

            for carrier, value in df.groupby("carrier")["capacity_gw"].sum().items():
                rows.append(
                    {
                        "scenario": scenario,
                        "component": "Link",
                        "carrier": rename_carrier(carrier),
                        "value": value,
                        "unit": "GW",
                    }
                )

        storage_unit_carriers = category_carriers.get("storage_units", [])
        if (
            storage_unit_carriers
            and not n.storage_units.empty
            and "p_nom_opt" in n.storage_units.columns
        ):
            df = n.storage_units[n.storage_units["carrier"].isin(storage_unit_carriers)]
            for carrier, value in df.groupby("carrier")["p_nom_opt"].sum().items():
                rows.append(
                    {
                        "scenario": scenario,
                        "component": "StorageUnit",
                        "carrier": rename_carrier(carrier),
                        "value": value / 1e3,
                        "unit": "GW",
                    }
                )

    return pd.DataFrame(rows)


def compute_annual_flow_by_carrier(
    networks: dict[str, pypsa.Network],
    category: str,
    mwh_per_tonne: dict[str, float],
) -> pd.DataFrame:
    """Compute annual production, demand, or capture by carrier in Mtpa."""
    rows = []

    for scenario, n in networks.items():
        w = get_snapshot_weightings(n)

        if category == "Hydrogen":
            link_carriers = [
                c
                for c in n.links.carrier.unique()
                if any(
                    k in c.lower()
                    for k in [
                        "electroly",
                        "smr",
                        "reforming",
                        "gasification",
                        "hydrogen",
                    ]
                )
            ]
            conversion = mwh_per_tonne["custom_h2"]

            links = n.links[n.links["carrier"].isin(link_carriers)]
            if links.empty:
                continue

            flows = pd.DataFrame(0.0, index=n.snapshots, columns=links.index)

            for link in links.index:
                if "p1" in n.links_t and link in n.links_t.p1.columns:
                    flows[link] = -n.links_t.p1[link].clip(upper=0)

        elif category == "Ammonia / e-ammonia":
            link_carriers = [
                "grey Haber-Bosch",
                "e Haber-Bosch",
            ]
            conversion = mwh_per_tonne["e_ammonia"]

            links = n.links[n.links["carrier"].isin(link_carriers)]
            if links.empty:
                continue

            flows = -n.links_t.p1[links.index].clip(upper=0)

        elif category == "Methanol / e-methanol":
            link_carriers = [
                "grey methanol synthesis",
                "e-methanol synthesis",
            ]
            conversion = mwh_per_tonne["e_methanol"]

            links = n.links[n.links["carrier"].isin(link_carriers)]
            if links.empty:
                continue

            flows = -n.links_t.p1[links.index].clip(upper=0)

        elif category == "CO2 capture":
            link_carriers = [
                "SMR CC",
            ]
            conversion = 1.0

            links = n.links[n.links["carrier"].isin(link_carriers)]
            if links.empty:
                continue

            flows = pd.DataFrame(0.0, index=n.snapshots, columns=links.index)

            for link in links.index:
                for bus_col in [c for c in n.links.columns if c.startswith("bus")]:
                    bus = str(n.links.at[link, bus_col])
                    if "co2 stored" not in bus.lower():
                        continue

                    p_col = f"p{bus_col.replace('bus', '')}"
                    if p_col in n.links_t and link in n.links_t[p_col].columns:
                        flows[link] += -n.links_t[p_col][link].clip(upper=0)

        else:
            return pd.DataFrame(
                columns=["scenario", "component", "carrier", "value", "unit"]
            )

        annual = flows.multiply(w, axis=0).sum()

        for carrier, value in annual.groupby(links["carrier"]).sum().items():
            rows.append(
                {
                    "scenario": scenario,
                    "component": "Link",
                    "carrier": rename_carrier(carrier),
                    "value": value / conversion / 1e6,
                    "unit": "Mtpa",
                }
            )

    return pd.DataFrame(rows)


def get_available_dispatch_categories() -> list[str]:
    """Return categories exposed in the dispatch explorer."""
    return [
        "Electricity",
        "Hydrogen",
        "Ammonia / Methanol",
    ]


def compute_dispatch_by_carrier(
    n: pypsa.Network,
    category: str,
) -> pd.DataFrame:
    """Compute dispatch time series by production technology."""
    if category == "Electricity":
        gen_carriers = [
            "solar",
            "solar rooftop",
            "onwind",
            "offwind-ac",
            "offwind-dc",
            "ror",
            "biomass",
            "coal",
            "lignite",
            "oil",
        ]

        link_carriers = [
            "OCGT",
            "CCGT",
            "coal",
            "lignite",
            "oil",
            "biomass",
            "battery discharger",
        ]

        frames = []

        gens = n.generators[n.generators["carrier"].isin(gen_carriers)]
        if not gens.empty:
            available = gens.index.intersection(n.generators_t.p.columns)

            if len(available) > 0:
                gen_dispatch = (
                    n.generators_t.p[available]
                    .clip(lower=0)
                    .groupby(n.generators.loc[available, "carrier"], axis=1)
                    .sum()
                )
                frames.append(gen_dispatch)

        storage_units = n.storage_units[
            n.storage_units["carrier"].isin(["PHS", "hydro"])
        ]
        if not storage_units.empty:
            available = storage_units.index.intersection(n.storage_units_t.p.columns)

            if len(available) > 0:
                storage_dispatch = (
                    n.storage_units_t.p[available]
                    .clip(lower=0)
                    .groupby(n.storage_units.loc[available, "carrier"], axis=1)
                    .sum()
                )
                frames.append(storage_dispatch)

        links = n.links[n.links["carrier"].isin(link_carriers)]
        if not links.empty and "p1" in n.links_t:
            available = links.index.intersection(n.links_t.p1.columns)

            if len(available) > 0:
                link_dispatch = (
                    -n.links_t.p1[available]
                    .clip(upper=0)
                    .groupby(n.links.loc[available, "carrier"], axis=1)
                    .sum()
                )
                frames.append(link_dispatch)

        if not frames:
            return pd.DataFrame()

        dispatch = pd.concat(frames, axis=1)
        dispatch = dispatch.groupby(dispatch.columns, axis=1).sum()
        dispatch = dispatch.rename(columns=rename_carrier)

        return dispatch / 1e3  # GW

    if category == "Hydrogen":
        specs = {
            "Alkaline electrolyzer large": 33.0,
            "Alkaline electrolyzer medium": 33.0,
            "Alkaline electrolyzer small": 33.0,
            "PEM electrolyzer": 33.0,
            "SOEC": 33.0,
            "SMR": 33.0,
            "SMR CC": 33.0,
            "Solid biomass steam reforming": 33.0,
            "Biomass gasification": 33.0,
            "Biomass gasification CC": 33.0,
            "Natural gas steam reforming": 33.0,
            "Natural gas steam reforming CC": 33.0,
            "Coal gasification": 33.0,
            "Coal gasification CC": 33.0,
            "Heavy oil partial oxidation": 33.0,
        }

    elif category == "Ammonia / Methanol":
        specs = {
            "grey Haber-Bosch": 5.17,
            "e Haber-Bosch": 5.17,
            "grey methanol synthesis": 5.54,
            "e-methanol synthesis": 5.54,
        }

    else:
        return pd.DataFrame()

    links = n.links[n.links["carrier"].isin(specs.keys())]

    if links.empty or "p1" not in n.links_t:
        return pd.DataFrame()

    frames = []

    for carrier, conversion in specs.items():
        carrier_links = links[links["carrier"] == carrier].index
        available = carrier_links.intersection(n.links_t.p1.columns)

        if len(available) == 0:
            continue

        dispatch = -n.links_t.p1[available].clip(upper=0).sum(axis=1)
        dispatch = dispatch.rename(rename_carrier(carrier)) / conversion / 1e3

        frames.append(dispatch)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, axis=1)  # kt/h


def compute_dispatch_annual_totals(
    n: pypsa.Network,
    dispatch_df: pd.DataFrame,
    category: str,
) -> pd.DataFrame:
    """Compute annual totals from dispatch time series."""
    if dispatch_df.empty:
        return pd.DataFrame(columns=["Carrier", "Value", "Unit"])

    w = get_snapshot_weightings(n)

    annual = dispatch_df.multiply(w, axis=0).sum()

    if category == "Electricity":
        unit = "TWh"
        values = annual / 1e3
    else:
        unit = "Mtpa"
        values = annual / 1e3

    return (
        values.rename("Value")
        .reset_index()
        .rename(columns={"index": "Carrier"})
        .assign(Unit=unit)
    )


def compute_lcoe_by_bus(network: pypsa.Network) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute plant-level LCOE and production-weighted LCOE by electricity cluster."""
    snapshot_weights = network.snapshot_weightings.generators

    gen_carriers = {
        "csp",
        "solar",
        "onwind",
        "offwind-dc",
        "offwind-ac",
        "nuclear",
        "geothermal",
        "ror",
        "hydro",
        "solar rooftop",
    }

    storage_carriers = {"battery storage", "hydro", "PHS"}

    link_carriers = [
        "coal",
        "oil",
        "OCGT",
        "CCGT",
        "biomass",
        "lignite",
        "urban central solid biomass CHP",
        "urban central gas CHP",
    ]

    electricity_cluster_buses = set(
        network.buses[network.buses.carrier.isin(["AC", "DC"])].index
    )

    rows = []

    # Generators.
    gen = network.generators[network.generators.carrier.isin(gen_carriers)].copy()
    if not gen.empty:
        gen_dispatch = network.generators_t.p[gen.index].multiply(
            snapshot_weights,
            axis=0,
        )
        gen["energy"] = gen_dispatch.sum()
        gen = gen[(gen.p_nom_opt > 0) & (gen.energy > 0)]

        gen["lcoe"] = (
            gen.capital_cost * gen.p_nom_opt + gen.marginal_cost * gen.energy
        ) / gen.energy

        gen["type"] = "generator"
        rows.append(gen[["bus", "carrier", "lcoe", "type", "energy"]])

    # Storage units.
    sto = network.storage_units[
        network.storage_units.carrier.isin(storage_carriers)
    ].copy()

    if not sto.empty:
        sto_dispatch = (
            network.storage_units_t.p[sto.index]
            .clip(lower=0)
            .multiply(snapshot_weights, axis=0)
        )
        sto["energy"] = sto_dispatch.sum()
        sto = sto[(sto.p_nom_opt > 0) & (sto.energy > 0)]

        sto["lcoe"] = (
            sto.capital_cost * sto.p_nom_opt + sto.marginal_cost * sto.energy
        ) / sto.energy

        sto["type"] = "storage"
        rows.append(sto[["bus", "carrier", "lcoe", "type", "energy"]])

    # Electricity-producing links.
    link = network.links[
        network.links.carrier.isin(link_carriers)
        & network.links.bus1.isin(electricity_cluster_buses)
        & (network.links.p_nom_opt > 0)
    ].copy()

    if not link.empty:
        link_dispatch = -network.links_t.p1[link.index].clip(upper=0)
        weighted_link_dispatch = link_dispatch.multiply(snapshot_weights, axis=0)
        link["energy"] = weighted_link_dispatch.sum()

        fuel_usage = network.links_t.p0[link.index].clip(lower=0)
        weighted_fuel_usage = fuel_usage.multiply(snapshot_weights, axis=0)
        link["fuel_usage"] = weighted_fuel_usage.sum()

        link["fuel_cost"] = link.bus0.map(network.generators.marginal_cost)

        hours = float(snapshot_weights.sum())
        link["CF"] = link["energy"] / (link["p_nom_opt"] * hours)

        def lcoe_link(row):
            if row["energy"] <= 0:
                return np.nan
            if row["carrier"] == "oil":
                return np.nan
            if row["CF"] < 0.05:
                return np.nan

            return (
                row["capital_cost"] * row["p_nom_opt"]
                + row["marginal_cost"] * row["fuel_usage"]
                + row["fuel_cost"] * row["fuel_usage"]
            ) / row["energy"]

        link["lcoe"] = link.apply(lcoe_link, axis=1)
        link["type"] = "link"

        rows.append(
            link[["bus1", "carrier", "lcoe", "type", "energy"]].rename(
                columns={"bus1": "bus"}
            )
        )

    if not rows:
        return pd.DataFrame(), pd.DataFrame()

    lcoe_data = pd.concat(rows, axis=0).dropna(subset=["bus", "lcoe", "energy"])

    # Keep only the physical electricity buses corresponding to the spatial clusters.
    # Sectoral buses share the same coordinates but are not separate plotted clusters.
    lcoe_data = lcoe_data[lcoe_data["bus"].isin(electricity_cluster_buses)]

    if lcoe_data.empty:
        return pd.DataFrame(), pd.DataFrame()

    lcoe_data = lcoe_data.merge(
        network.buses[["x", "y"]],
        left_on="bus",
        right_index=True,
    )

    lcoe_by_bus = (
        lcoe_data.groupby("bus")
        .apply(
            lambda df: pd.Series(
                {
                    "weighted_lcoe": (df["lcoe"] * df["energy"]).sum()
                    / df["energy"].sum(),
                    "dispatch_twh": df["energy"].sum() / 1e6,
                    "x": df["x"].iloc[0],
                    "y": df["y"].iloc[0],
                }
            )
        )
        .reset_index()
    )

    return lcoe_by_bus, lcoe_data


def plot_lcoe_map_by_bus(
    lcoe_by_bus: pd.DataFrame,
    shapes: gpd.GeoDataFrame,
    title: str | None = None,
    ax=None,
    vmin: float | None = None,
    vmax: float | None = None,
):
    """Plot production-weighted LCOE by electricity cluster over a map background."""
    if lcoe_by_bus.empty:
        return None

    shapes = shapes.to_crs("EPSG:4326")

    if vmin is None:
        vmin = lcoe_by_bus["weighted_lcoe"].quantile(0.05)

    if vmax is None:
        vmax = lcoe_by_bus["weighted_lcoe"].quantile(0.95)

    if ax is None:
        fig, ax = plt.subplots(figsize=(5.0, 3.5))
    else:
        fig = ax.figure

    # Geographic background only.
    shapes.plot(
        ax=ax,
        facecolor="whitesmoke",
        edgecolor="0.7",
        linewidth=0.5,
        zorder=1,
    )

    max_dispatch = lcoe_by_bus["dispatch_twh"].max()
    if max_dispatch > 0:
        sizes = 200 * np.sqrt(lcoe_by_bus["dispatch_twh"] / max_dispatch)
    else:
        sizes = 80

    scatter = ax.scatter(
        lcoe_by_bus["x"],
        lcoe_by_bus["y"],
        c=lcoe_by_bus["weighted_lcoe"],
        s=sizes,
        cmap="RdYlGn_r",
        vmin=vmin,
        vmax=vmax,
        marker="o",
        linewidths=0,
        edgecolors="none",
        alpha=1.0,
        zorder=5,
    )

    cbar = fig.colorbar(
        scatter,
        ax=ax,
        shrink=0.75,
        pad=0.02,
    )

    cbar.set_label(
        "Generation-weighted LCOE (AUD/MWh)",
        fontsize=6,
    )

    cbar.ax.tick_params(labelsize=6)

    # Bubble-size legend for annual generation.
    legend_values = [
        lcoe_by_bus["dispatch_twh"].quantile(0.25),
        lcoe_by_bus["dispatch_twh"].quantile(0.50),
        lcoe_by_bus["dispatch_twh"].quantile(0.90),
    ]

    legend_values = [2, 10, 50]

    legend_handles = []

    for value in legend_values:
        marker_size = 200 * np.sqrt(value / max_dispatch)

        legend_handles.append(
            ax.scatter(
                [],
                [],
                s=marker_size,
                color="lightgray",
                edgecolors="gray",
                linewidths=0.1,
                label=f"{value:.1f} TWh",
            )
        )

    if legend_handles:
        ax.legend(
            handles=legend_handles,
            title="Annual generation",
            loc="lower left",
            frameon=False,
            fontsize=6,
            title_fontsize=6,
            labelspacing=1.4,
            borderpad=0.8,
            handletextpad=1.0,
        )

    ax.set_xlim(110, 155)
    ax.set_ylim(-45, -10)
    ax.axis("off")

    fig.tight_layout()

    return fig


def compute_lcoh_by_bus(network: pypsa.Network) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute production-weighted LCOH for grid H2 by electricity cluster."""
    snapshot_weights = network.snapshot_weightings.generators

    h2_output_buses = network.buses[network.buses.carrier == "grid H2"].index

    h2_links = network.links[
        network.links.bus1.isin(h2_output_buses) & (network.links.p_nom_opt > 0)
    ].copy()

    if h2_links.empty:
        return pd.DataFrame(), pd.DataFrame()

    h2_dispatch = (-network.links_t.p1[h2_links.index].clip(upper=0)).multiply(
        snapshot_weights,
        axis=0,
    )

    h2_links["h2_output_mwh"] = h2_dispatch.sum()
    h2_links = h2_links[h2_links["h2_output_mwh"] > 0]

    if h2_links.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Electricity input to H2 production.
    electricity_input = (
        network.links_t.p0[h2_links.index]
        .clip(lower=0)
        .multiply(
            snapshot_weights,
            axis=0,
        )
    )

    # Use local marginal electricity price on bus0.
    electricity_cost = {}
    for link_name, row in h2_links.iterrows():
        bus0 = row["bus0"]

        if bus0 in network.buses_t.marginal_price.columns:
            electricity_cost[link_name] = (
                electricity_input[link_name] * network.buses_t.marginal_price[bus0]
            ).sum()
        else:
            electricity_cost[link_name] = 0.0

    h2_links["electricity_cost"] = pd.Series(electricity_cost)

    h2_links["capital_cost_total"] = h2_links["capital_cost"] * h2_links["p_nom_opt"]

    h2_links["marginal_cost_total"] = (
        h2_links["marginal_cost"] * h2_links["h2_output_mwh"]
    )

    h2_links["total_cost"] = (
        h2_links["capital_cost_total"]
        + h2_links["marginal_cost_total"]
        + h2_links["electricity_cost"]
    )

    h2_links["lcoh_aud_per_mwh"] = h2_links["total_cost"] / h2_links["h2_output_mwh"]

    # Convert AUD/MWh_H2 to AUD/kg_H2 using 33 kWh/kg.
    h2_links["lcoh_aud_per_kg"] = h2_links["lcoh_aud_per_mwh"] * 33.0 / 1000.0

    lcoh_data = h2_links[
        [
            "bus0",
            "bus1",
            "carrier",
            "h2_output_mwh",
            "lcoh_aud_per_kg",
            "lcoh_aud_per_mwh",
            "capital_cost_total",
            "marginal_cost_total",
            "electricity_cost",
        ]
    ].copy()

    lcoh_data = lcoh_data.rename(columns={"bus1": "h2_bus"})

    # Map grid H2 bus back to the physical cluster location.
    lcoh_data["cluster"] = lcoh_data["h2_bus"].str.replace(" grid H2", "", regex=False)

    lcoh_data = lcoh_data.merge(
        network.buses[["x", "y"]],
        left_on="cluster",
        right_index=True,
        how="left",
    )

    lcoh_data = lcoh_data.dropna(subset=["x", "y"])

    lcoh_by_bus = (
        lcoh_data.groupby("cluster")
        .apply(
            lambda df: pd.Series(
                {
                    "weighted_lcoh_aud_per_kg": (
                        df["lcoh_aud_per_kg"] * df["h2_output_mwh"]
                    ).sum()
                    / df["h2_output_mwh"].sum(),
                    "weighted_lcoh_aud_per_mwh": (
                        df["lcoh_aud_per_mwh"] * df["h2_output_mwh"]
                    ).sum()
                    / df["h2_output_mwh"].sum(),
                    "h2_dispatch_twh": df["h2_output_mwh"].sum() / 1e6,
                    "h2_dispatch_kt": df["h2_output_mwh"].sum() / 33.0 / 1e3,
                    "x": df["x"].iloc[0],
                    "y": df["y"].iloc[0],
                }
            )
        )
        .reset_index()
    )

    return lcoh_by_bus, lcoh_data


def plot_lcoh_map_by_bus(
    lcoh_by_bus: pd.DataFrame,
    shapes: gpd.GeoDataFrame,
    title: str | None = None,
    ax=None,
    vmin: float | None = None,
    vmax: float | None = None,
):
    """Plot production-weighted LCOH by electricity cluster over a map background."""
    if lcoh_by_bus.empty:
        return None

    shapes = shapes.to_crs("EPSG:4326")

    if vmin is None:
        vmin = lcoh_by_bus["weighted_lcoh_aud_per_kg"].quantile(0.05)

    if vmax is None:
        vmax = lcoh_by_bus["weighted_lcoh_aud_per_kg"].quantile(0.95)

    if ax is None:
        fig, ax = plt.subplots(figsize=(5.0, 3.5))
    else:
        fig = ax.figure

    shapes.plot(
        ax=ax,
        facecolor="whitesmoke",
        edgecolor="0.7",
        linewidth=0.5,
        zorder=1,
    )

    max_dispatch = lcoh_by_bus["h2_dispatch_kt"].max()
    if max_dispatch > 0:
        sizes = 200 * np.sqrt(lcoh_by_bus["h2_dispatch_kt"] / max_dispatch)
    else:
        sizes = 80

    scatter = ax.scatter(
        lcoh_by_bus["x"],
        lcoh_by_bus["y"],
        c=lcoh_by_bus["weighted_lcoh_aud_per_kg"],
        s=sizes,
        cmap="RdYlGn_r",
        vmin=vmin,
        vmax=vmax,
        marker="o",
        linewidths=0,
        edgecolors="none",
        alpha=1.0,
        zorder=5,
    )

    cbar = fig.colorbar(
        scatter,
        ax=ax,
        shrink=0.75,
        pad=0.02,
    )

    cbar.set_label(
        "Production-weighted LCOH (AUD/kg H2)",
        fontsize=6,
    )

    cbar.ax.tick_params(labelsize=6)

    legend_values = [2, 10, 50]
    legend_handles = []

    for value in legend_values:
        marker_size = 200 * np.sqrt(value / max_dispatch)

        legend_handles.append(
            ax.scatter(
                [],
                [],
                s=marker_size,
                color="lightgray",
                edgecolors="gray",
                linewidths=0.1,
                label=f"{value:.0f} kt H2/year",
            )
        )

    if legend_handles:
        ax.legend(
            handles=legend_handles,
            title="Annual H2 production",
            loc="lower left",
            frameon=False,
            fontsize=6,
            title_fontsize=6,
            labelspacing=1.4,
            borderpad=0.8,
            handletextpad=1.0,
        )

    ax.set_xlim(110, 155)
    ax.set_ylim(-45, -10)
    ax.axis("off")

    fig.tight_layout()

    return fig


def compute_lco_product_by_bus(
    network: pypsa.Network,
    product_bus_carrier: str,
    product_label: str,
    mwh_per_tonne: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute production-weighted levelized cost for a product by cluster."""
    snapshot_weights = network.snapshot_weightings.generators

    product_buses = network.buses[network.buses.carrier == product_bus_carrier].index

    product_links = network.links[
        network.links.bus1.isin(product_buses) & (network.links.p_nom_opt > 0)
    ].copy()

    if product_links.empty:
        return pd.DataFrame(), pd.DataFrame()

    output = (-network.links_t.p1[product_links.index].clip(upper=0)).multiply(
        snapshot_weights,
        axis=0,
    )

    product_links["output_mwh"] = output.sum()
    product_links = product_links[product_links["output_mwh"] > 0]

    if product_links.empty:
        return pd.DataFrame(), pd.DataFrame()

    total_input_cost = pd.Series(0.0, index=product_links.index)

    bus_cols = [c for c in product_links.columns if c.startswith("bus")]

    for bus_col in bus_cols:
        bus_nr = bus_col.replace("bus", "")
        flow_col = f"p{bus_nr}"

        if not hasattr(network.links_t, flow_col):
            continue

        flows = getattr(network.links_t, flow_col)

        available_links = product_links.index.intersection(flows.columns)
        if available_links.empty:
            continue

        input_flows = (
            flows[available_links]
            .clip(lower=0)
            .multiply(
                snapshot_weights,
                axis=0,
            )
        )

        for link_name in available_links:
            bus = product_links.at[link_name, bus_col]

            if bus not in network.buses_t.marginal_price.columns:
                continue

            total_input_cost.loc[link_name] += (
                input_flows[link_name] * network.buses_t.marginal_price[bus]
            ).sum()

    product_links["input_cost"] = total_input_cost

    product_links["capital_cost_total"] = (
        product_links["capital_cost"] * product_links["p_nom_opt"]
    )

    product_links["marginal_cost_total"] = (
        product_links["marginal_cost"] * product_links["output_mwh"]
    )

    product_links["total_cost"] = (
        product_links["capital_cost_total"]
        + product_links["marginal_cost_total"]
        + product_links["input_cost"]
    )

    product_links["cost_aud_per_mwh"] = (
        product_links["total_cost"] / product_links["output_mwh"]
    )

    product_links["cost_aud_per_tonne"] = (
        product_links["cost_aud_per_mwh"] * mwh_per_tonne
    )

    product_data = product_links[
        [
            "bus1",
            "carrier",
            "output_mwh",
            "cost_aud_per_mwh",
            "cost_aud_per_tonne",
            "capital_cost_total",
            "marginal_cost_total",
            "input_cost",
        ]
    ].copy()

    product_data = product_data.rename(columns={"bus1": "product_bus"})

    product_data["cluster"] = product_data["product_bus"].str.replace(
        f" {product_bus_carrier}",
        "",
        regex=False,
    )

    product_data = product_data.merge(
        network.buses[["x", "y"]],
        left_on="cluster",
        right_index=True,
        how="left",
    )

    product_data = product_data.dropna(subset=["x", "y"])

    product_by_bus = (
        product_data.groupby("cluster")
        .apply(
            lambda df: pd.Series(
                {
                    f"weighted_lco_{product_label}_aud_per_tonne": (
                        df["cost_aud_per_tonne"] * df["output_mwh"]
                    ).sum()
                    / df["output_mwh"].sum(),
                    f"weighted_lco_{product_label}_aud_per_mwh": (
                        df["cost_aud_per_mwh"] * df["output_mwh"]
                    ).sum()
                    / df["output_mwh"].sum(),
                    "production_twh": df["output_mwh"].sum() / 1e6,
                    "production_kt": df["output_mwh"].sum() / mwh_per_tonne / 1e3,
                    "x": df["x"].iloc[0],
                    "y": df["y"].iloc[0],
                }
            )
        )
        .reset_index()
    )

    return product_by_bus, product_data


def compute_lco_ammonia_by_bus(network: pypsa.Network):
    """Compute production-weighted levelized cost for e-ammonia."""
    return compute_lco_product_by_bus(
        network=network,
        product_bus_carrier="e-ammonia",
        product_label="ammonia",
        mwh_per_tonne=5.17,
    )


def compute_lco_methanol_by_bus(network: pypsa.Network):
    """Compute production-weighted levelized cost for e-methanol."""
    return compute_lco_product_by_bus(
        network=network,
        product_bus_carrier="e-methanol",
        product_label="methanol",
        mwh_per_tonne=5.54,
    )


def plot_lco_product_map_by_bus(
    product_by_bus: pd.DataFrame,
    shapes: gpd.GeoDataFrame,
    value_col: str,
    cbar_label: str,
    legend_title: str,
    legend_unit: str,
    title: str | None = None,
    ax=None,
    vmin: float | None = None,
    vmax: float | None = None,
):
    """Plot production-weighted product cost by cluster over a map background."""
    if product_by_bus.empty:
        return None

    shapes = shapes.to_crs("EPSG:4326")

    if vmin is None:
        vmin = product_by_bus[value_col].quantile(0.05)

    if vmax is None:
        vmax = product_by_bus[value_col].quantile(0.95)

    if ax is None:
        fig, ax = plt.subplots(figsize=(5.0, 3.5))
    else:
        fig = ax.figure

    shapes.plot(
        ax=ax,
        facecolor="whitesmoke",
        edgecolor="0.7",
        linewidth=0.5,
        zorder=1,
    )

    max_dispatch = product_by_bus["production_kt"].max()

    if max_dispatch > 0:
        sizes = 200 * np.sqrt(product_by_bus["production_kt"] / max_dispatch)
    else:
        sizes = 80

    scatter = ax.scatter(
        product_by_bus["x"],
        product_by_bus["y"],
        c=product_by_bus[value_col],
        s=sizes,
        cmap="RdYlGn_r",
        vmin=vmin,
        vmax=vmax,
        marker="o",
        linewidths=0,
        edgecolors="none",
        alpha=1.0,
        zorder=5,
    )

    cbar = fig.colorbar(
        scatter,
        ax=ax,
        shrink=0.75,
        pad=0.02,
    )

    cbar.set_label(
        cbar_label,
        fontsize=6,
    )

    cbar.ax.tick_params(labelsize=6)

    legend_values = [2, 10, 50]
    legend_handles = []

    for value in legend_values:
        marker_size = 200 * np.sqrt(value / max_dispatch)

        legend_handles.append(
            ax.scatter(
                [],
                [],
                s=marker_size,
                color="lightgray",
                edgecolors="gray",
                linewidths=0.1,
                label=f"{value:.0f} {legend_unit}/year",
            )
        )

    if legend_handles:
        ax.legend(
            handles=legend_handles,
            title=legend_title,
            loc="lower left",
            frameon=False,
            fontsize=6,
            title_fontsize=6,
            labelspacing=1.4,
            borderpad=0.8,
            handletextpad=1.0,
        )

    ax.set_xlim(110, 155)
    ax.set_ylim(-45, -10)
    ax.axis("off")

    if title:
        ax.set_title(title, fontsize=10)

    fig.tight_layout()

    return fig


def plot_lco_ammonia_map_by_bus(ammonia_by_bus, shapes, title=None):
    return plot_lco_product_map_by_bus(
        product_by_bus=ammonia_by_bus,
        shapes=shapes,
        value_col="weighted_lco_ammonia_aud_per_tonne",
        cbar_label="Production-weighted LCOA (AUD/t NH3)",
        legend_title="Annual e-ammonia production",
        legend_unit="kt NH3",
        title=title,
    )


def plot_lco_methanol_map_by_bus(methanol_by_bus, shapes, title=None):
    return plot_lco_product_map_by_bus(
        product_by_bus=methanol_by_bus,
        shapes=shapes,
        value_col="weighted_lco_methanol_aud_per_tonne",
        cbar_label="Production-weighted LCOMeOH (AUD/t MeOH)",
        legend_title="Annual e-methanol production",
        legend_unit="kt MeOH",
        title=title,
    )
