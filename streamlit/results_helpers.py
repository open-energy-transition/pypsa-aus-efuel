# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pypsa
from matplotlib.patches import Patch


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
                "Alkaline electrolyzer small",
                "Alkaline electrolyzer medium",
                "Alkaline electrolyzer large",
                "SOEC",
                "PEM electrolyzer",
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
    """Compute optimized output-side capacity by exact carrier for selected scenarios."""
    category_carriers = get_category_carriers(category)
    rows = []

    for scenario, n in networks.items():
        generator_carriers = category_carriers.get("generators", [])
        if (
            generator_carriers
            and not n.generators.empty
            and "p_nom_opt" in n.generators.columns
        ):
            df = n.generators[n.generators["carrier"].isin(generator_carriers)].copy()
            df["capacity_gw"] = df["p_nom_opt"] / 1e3

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
            df = n.storage_units[
                n.storage_units["carrier"].isin(storage_unit_carriers)
            ].copy()
            df["capacity_gw"] = df["p_nom_opt"] / 1e3

            for carrier, value in df.groupby("carrier")["capacity_gw"].sum().items():
                rows.append(
                    {
                        "scenario": scenario,
                        "component": "StorageUnit",
                        "carrier": rename_carrier(carrier),
                        "value": value,
                        "unit": "GW",
                    }
                )

    return pd.DataFrame(rows)


def compute_capacity_by_bus(
    network: pypsa.Network,
    category: str,
) -> pd.DataFrame:
    """Compute optimized installed/output capacity by physical cluster and carrier."""
    category_carriers = get_category_carriers(category)
    rows = []

    generator_carriers = category_carriers.get("generators", [])
    if (
        generator_carriers
        and not network.generators.empty
        and "p_nom_opt" in network.generators.columns
    ):
        df = network.generators[
            network.generators["carrier"].isin(generator_carriers)
        ].copy()

        df["value"] = df["p_nom_opt"] / 1e3
        df["cluster"] = df["bus"]

        rows.append(df[["cluster", "carrier", "value"]])

    link_carriers = category_carriers.get("links", [])
    if (
        link_carriers
        and not network.links.empty
        and "p_nom_opt" in network.links.columns
    ):
        df = network.links[network.links["carrier"].isin(link_carriers)].copy()

        df["value"] = df["p_nom_opt"] * df["efficiency"].fillna(1.0) / 1e3
        df["cluster"] = df["bus1"]

        rows.append(df[["cluster", "carrier", "value"]])

    storage_unit_carriers = category_carriers.get("storage_units", [])
    if (
        storage_unit_carriers
        and not network.storage_units.empty
        and "p_nom_opt" in network.storage_units.columns
    ):
        df = network.storage_units[
            network.storage_units["carrier"].isin(storage_unit_carriers)
        ].copy()

        df["value"] = df["p_nom_opt"] / 1e3
        df["cluster"] = df["bus"]

        rows.append(df[["cluster", "carrier", "value"]])

    if not rows:
        return pd.DataFrame()

    capacity = pd.concat(rows, axis=0)
    capacity["carrier"] = capacity["carrier"].map(rename_carrier)

    capacity["plot_cluster"] = (
        capacity["cluster"]
        .str.replace(" grey-ammonia", "", regex=False)
        .str.replace(" e-ammonia", "", regex=False)
        .str.replace(" grey-methanol", "", regex=False)
        .str.replace(" e-methanol", "", regex=False)
        .str.replace(" grid H2", "", regex=False)
    )

    capacity = (
        capacity.groupby(["plot_cluster", "carrier"], as_index=False)["value"]
        .sum()
        .rename(columns={"plot_cluster": "cluster"})
    )

    capacity = capacity.merge(
        network.buses[["x", "y"]],
        left_on="cluster",
        right_index=True,
        how="left",
    )

    capacity = capacity.dropna(subset=["x", "y"])
    capacity = capacity[capacity["value"] > 0]

    return capacity


def plot_capacity_map_by_bus(
    capacity_by_bus: pd.DataFrame,
    shapes: gpd.GeoDataFrame,
    color_map: dict[str, str],
    unit: str = "GW",
    title: str | None = None,
    ax=None,
):
    """Plot stacked production-capacity bars by cluster on a map."""
    if capacity_by_bus.empty:
        return None

    shapes = shapes.to_crs("EPSG:4326")

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

    totals = capacity_by_bus.groupby("cluster")["value"].sum()
    max_total = totals.max()

    if max_total <= 0:
        return fig

    max_bar_height = 8.0
    bar_width = 0.75
    label_threshold = 15.0 if unit == "GW" else 0.1

    carriers = [
        carrier
        for carrier in color_map
        if carrier in capacity_by_bus["carrier"].unique()
    ]

    for cluster, group in capacity_by_bus.groupby("cluster"):
        x = group["x"].iloc[0]
        y = group["y"].iloc[0]
        bottom = y

        group = (
            group.groupby("carrier", as_index=False)["value"].sum().set_index("carrier")
        )

        for carrier in carriers:
            if carrier not in group.index:
                continue

            value = group.loc[carrier, "value"]

            if value <= 0:
                continue

            scale_factor = 1.0 if unit == "GW" else 0.55

            height = value / max_total * max_bar_height * scale_factor

            ax.bar(
                x,
                height,
                width=bar_width,
                bottom=bottom,
                color=color_map.get(carrier, "gray"),
                edgecolor="none",
                align="center",
                zorder=5,
            )

            if value >= label_threshold:
                ax.text(
                    x + 0.8,
                    bottom + height / 2,
                    f"{value:.1f} {unit}",
                    fontsize=6,
                    color=color_map.get(carrier, "black"),
                    fontweight="bold",
                    ha="left",
                    va="center",
                    zorder=20,
                )

            bottom += height

    legend_handles = [
        Patch(
            facecolor=color_map.get(carrier, "gray"),
            edgecolor="none",
            label=carrier,
        )
        for carrier in carriers
    ]

    if legend_handles:
        ax.legend(
            handles=legend_handles,
            title="Technology",
            loc="center left",
            bbox_to_anchor=(1.20, 0.5),
            frameon=False,
            fontsize=6,
            title_fontsize=6,
        )

    ax.set_xlim(110, 155)
    ax.set_ylim(-45, -10)
    ax.axis("off")

    if title:
        ax.set_title(title, fontsize=9, fontweight="bold")

    fig.tight_layout()

    return fig


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
                if any(k in c.lower() for k in ["electroly", "soec"])
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
            # "SMR": 33.0,
            # "SMR CC": 33.0,
            # "Solid biomass steam reforming": 33.0,
            # "Biomass gasification": 33.0,
            # "Biomass gasification CC": 33.0,
            # "Natural gas steam reforming": 33.0,
            # "Natural gas steam reforming CC": 33.0,
            # "Coal gasification": 33.0,
            # "Coal gasification CC": 33.0,
            # "Heavy oil partial oxidation": 33.0,
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


# SYSTEM COST CONFIGURATION

rename_tech_capex = {
    # Wind
    "Onshore Wind": "Wind",
    "onwind": "Wind",
    "Offshore Wind (AC)": "Wind",
    "offwind-ac": "Wind",
    "Offshore Wind (DC)": "Wind",
    "offwind-dc": "Wind",
    # Solar
    "Solar": "Solar",
    "solar": "Solar",
    "solar rooftop": "Solar",
    "Csp": "Solar",
    "csp": "Solar",
    # Fossil generators
    "Open-Cycle Gas": "Fossil generators",
    "Combined-Cycle Gas": "Fossil generators",
    "urban central gas CHP": "Fossil generators",
    # Fossil fuels / supply
    "coal": "Fossil fuels",
    "oil": "Fossil fuels",
    "gas": "Fossil fuels",
    # Biomass
    "biomass": "Biomass fuels",
    "Biomass": "Biomass fuels",
    "biomass EOP": "Biomass generators",
    "urban central solid biomass CHP": "Biomass generators",
    "solid biomass": "Biomass fuels",
    "biogas": "Biomass fuels",
    # Hydro
    "Run of River": "Hydropower",
    "ror": "Hydropower",
    "Pumped Hydro Storage": "Hydropower",
    "PHS": "Hydropower",
    "Reservoir & Dam": "Hydropower",
    "hydro": "Hydropower",
    # Storage
    "Battery": "Electricity Storage",
    "battery": "Electricity Storage",
    "Battery Storage": "Electricity Storage",
    "battery storage": "Electricity Storage",
    "battery charger": "Electricity Storage",
    "battery discharger": "Electricity Storage",
    "battery inverter": "Electricity Storage",
    "home battery": "Electricity Storage",
    "home battery charger": "Electricity Storage",
    "home battery discharger": "Electricity Storage",
    "Li ion": "Electricity Storage",
    "BEV charger": "Electricity Storage",
    "grid H2 Store Tank": "Hydrogen Storage",
    "H2 Store Tank": "Hydrogen Storage",
    # Transmission
    "electricity distribution grid": "Power distribution",
    "AC": "Power transmission",
    "Ac": "Power transmission",
    "DC": "Power transmission",
    "Dc": "Power transmission",
    "B2B": "Power transmission",
    "V2G": "Power transmission",
    "CO2 pipeline": "CO2 transport",
    "solid biomass transport": "Biomass transport",
    # Hydrogen & e-fuels
    "SOEC": "Hydrogen",
    "PEM electrolyzer": "Hydrogen",
    "Alkaline electrolyzer large": "Hydrogen",
    "Alkaline electrolyzer medium": "Hydrogen",
    "Alkaline electrolyzer small": "Hydrogen",
    "H2 pipeline": "Hydrogen",
    "H2 pipeline repurposed": "Hydrogen",
    "grid H2": "Hydrogen",
    "H2": "Hydrogen",
    "grey H2": "Hydrogen",
    "blue H2": "Hydrogen",
    "H2 Fuel Cell": "Hydrogen",
    "Fischer-Tropsch": "e-fuels synthesis",
    "Haber-Bosch": "e-fuels synthesis",
    "grey Haber-Bosch": "e-fuels synthesis",
    "e Haber-Bosch": "e-fuels synthesis",
    "Sabatier": "e-fuels synthesis",
    "grey methanol synthesis": "e-fuels synthesis",
    "e-methanol synthesis": "e-fuels synthesis",
    "methanol": "e-fuels synthesis",
    "e-methanol": "e-fuels synthesis",
    "ammonia": "e-fuels synthesis",
    "e-ammonia": "e-fuels synthesis",
    "grey-ammonia": "e-fuels synthesis",
    "grey-methanol": "e-fuels synthesis",
    # Industry
    "SMR": "Industry",
    "SMR CC": "Industry CC",
    "gas for industry": "Industry",
    "solid biomass for industry": "Industry",
    "naphtha for industry": "Industry",
    "industry electricity": "Industry",
    # Carbon capture/storage/emissions
    "DAC": "DAC",
    "co2 stored": "CO2 Storage",
    "co2": "Emissions",
    "industry coal emissions": "Emissions",
    "industry oil emissions": "Emissions",
    "process emissions": "Emissions",
    "process emissions CC": "Emissions",
    # Other generators
    "Nuclear": "Nuclear",
    "nuclear": "Nuclear",
    "Geothermal": "Geothermal",
    "geothermal": "Geothermal",
    # Demand/end-use
    "custom H2 demand supply": "End-uses",
    # Fallback
    "-": "Others",
}

rename_tech_opex = rename_tech_capex.copy()

rename_tech_opex.update(
    {
        "CCGT": "Fossil generators",
        "OCGT": "Fossil generators",
    }
)

renamed_tech_colors = {
    "Biofuels synthesis": "#66c2a5",
    "Emissions": "#7f7f7f",
    "Fossil generators": "#8c564b",
    "Fossil fuels": "#a0522d",
    "Fossil fuels (end uses)": "#5e3b34",
    "Heating": "#d62728",
    "Hydrogen": "#1f77b4",
    "Industry": "#9467bd",
    "Industry CC": "#373170",
    "Power transmission": "#cda434",
    "Power distribution": "#ff7e33",
    "CO2 transport": "#1d4cdb",
    "Biomass transport": "#bae38a",
    "Others": "#c7c7c7",
    "CO2 Storage": "#94ffea",
    "Electricity Storage": "#fffd94",
    "Hydrogen Storage": "#17becf",
    "Transport": "#ff7f0e",
    "Biomass generators": "#2ca02c",
    "Biomass fuels": "#3cb371",
    "Biomass fuels (end uses)": "#2ca02c",
    "Geothermal": "#e35812",
    "Hydropower": "#298c81",
    "Nuclear": "#e8a9d5",
    "Solar": "#ffdd57",
    "Wind": "#a6cee3",
    "e-fuels synthesis": "#73ffb2",
    "End-uses": "#5e3b34",
}

categories_capex = {
    "Fossil generators": "Power & heat generation",
    "Biomass generators": "Power & heat generation",
    "Hydropower": "Power & heat generation",
    "Nuclear": "Power & heat generation",
    "Geothermal": "Power & heat generation",
    "Wind": "Power & heat generation",
    "Solar": "Power & heat generation",
    "Heating": "Power & heat generation",
    "Fossil fuels": "End-uses",
    "Biomass fuels": "End-uses",
    "Electricity Storage": "Storage",
    "CO2 Storage": "Storage",
    "Hydrogen Storage": "Storage",
    "Industry": "Industry",
    "Industry CC": "Industry",
    "DAC": "DAC",
    "Power transmission": "Transmission & distribution",
    "Power distribution": "Transmission & distribution",
    "CO2 transport": "Transmission & distribution",
    "Biomass transport": "Transmission & distribution",
    "Emissions": "Emissions",
    "Hydrogen": "Hydrogen & e-fuels",
    "e-fuels synthesis": "Hydrogen & e-fuels",
    "Biofuels synthesis": "Biofuels synthesis",
    "End-uses": "End-uses",
    "Others": "Others",
}

categories_opex = categories_capex.copy()


def assign_macro_category(row, categories_capex, categories_opex):
    if row["cost_type"] == "Capital expenditure":
        return categories_capex.get(row["tech_label"], "Others")

    if row["cost_type"] == "Operational expenditure":
        return categories_opex.get(row["tech_label"], "Others")

    return "Others"


def compute_system_costs(network, rename_capex, rename_opex, name_tag):
    """
    Compute CAPEX and OPEX including input costs.
    """

    def clean_raw_technology(series):
        return (
            series.astype(str)
            .str.strip()
            .replace(
                {
                    "Ac": "AC",
                    "Dc": "DC",
                    "Coal": "coal",
                    "Oil": "oil",
                    "Biomass": "biomass",
                    "Combined-Cycle Gas": "CCGT",
                    "Open-Cycle Gas": "OCGT",
                }
            )
        )

    costs_raw = network.statistics()[["Capital Expenditure", "Operational Expenditure"]]

    # CAPEX
    capex_raw = costs_raw[["Capital Expenditure"]].reset_index()

    carrier_col = "carrier" if "carrier" in capex_raw.columns else capex_raw.columns[1]

    capex_raw["raw_technology"] = clean_raw_technology(capex_raw[carrier_col])

    capex_raw["tech_label"] = (
        capex_raw[carrier_col].map(rename_capex).fillna(capex_raw[carrier_col])
    )

    capex_grouped = (
        capex_raw.groupby("tech_label", as_index=False)
        .agg(
            {
                "Capital Expenditure": "sum",
                "raw_technology": lambda x: ", ".join(sorted(set(x.astype(str)))),
            }
        )
        .rename(columns={"Capital Expenditure": "cost_billion"})
    )

    capex_grouped["cost_billion"] /= 1e9
    capex_grouped["cost_type"] = "Capital expenditure"
    capex_grouped["scenario"] = name_tag

    # OPEX
    opex_raw = costs_raw[["Operational Expenditure"]].reset_index()

    carrier_col = "carrier" if "carrier" in opex_raw.columns else opex_raw.columns[1]

    opex_raw["raw_technology"] = clean_raw_technology(opex_raw[carrier_col])

    opex_raw["tech_label"] = (
        opex_raw[carrier_col].map(rename_opex).fillna(opex_raw[carrier_col])
    )

    opex_grouped = (
        opex_raw.groupby("tech_label", as_index=False)
        .agg(
            {
                "Operational Expenditure": "sum",
                "raw_technology": lambda x: ", ".join(sorted(set(x.astype(str)))),
            }
        )
        .rename(columns={"Operational Expenditure": "cost_billion"})
    )

    opex_grouped["cost_billion"] /= 1e9
    opex_grouped["cost_type"] = "Operational expenditure"
    opex_grouped["scenario"] = name_tag

    # EXTRA INPUT OPEX
    if "objective" in network.snapshot_weightings.columns:
        w = network.snapshot_weightings["objective"]
    else:
        w = network.snapshot_weightings["generators"]

    bus_cols = [c for c in network.links.columns if c.startswith("bus")]

    results_extra = []

    for link_id, row in network.links.iterrows():
        tech = row["carrier"]

        for bcol in bus_cols:
            bus = row[bcol]

            if pd.isna(bus):
                continue

            if bus not in network.buses_t.marginal_price.columns:
                continue

            idx = bcol[3:]
            pcol = f"p{idx}"

            if pcol not in network.links_t:
                continue

            if link_id not in network.links_t[pcol]:
                continue

            flow_ts = network.links_t[pcol][link_id].clip(lower=0)

            if flow_ts.abs().sum() <= 0:
                continue

            prices = network.buses_t.marginal_price[bus]
            fuel_cost = (flow_ts * prices * w).sum()

            if fuel_cost <= 0:
                continue

            results_extra.append(
                {
                    "tech_label": rename_opex.get(tech, tech),
                    "raw_technology": clean_raw_technology(pd.Series([tech])).iloc[0],
                    "cost_billion": fuel_cost / 1e9,
                    "cost_type": "Operational expenditure",
                    "scenario": name_tag,
                }
            )

    extra_df = pd.DataFrame(results_extra)

    df_all = pd.concat(
        [capex_grouped, opex_grouped, extra_df],
        ignore_index=True,
    )

    return df_all


def build_system_cost_table(networks):
    """
    Build aggregated system cost table across scenarios.
    """

    all_costs = []

    for name_tag, network in networks.items():
        df = compute_system_costs(
            network=network,
            rename_capex=rename_tech_capex,
            rename_opex=rename_tech_opex,
            name_tag=name_tag,
        )

        all_costs.append(df)

    df_all = pd.concat(all_costs, ignore_index=True)

    df_all["tech_label"] = (
        df_all["tech_label"]
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"\xa0", " ", regex=True)
        .str.strip()
    )

    df_all["macro_category"] = df_all.apply(
        lambda row: assign_macro_category(
            row,
            categories_capex,
            categories_opex,
        ),
        axis=1,
    )

    return df_all
