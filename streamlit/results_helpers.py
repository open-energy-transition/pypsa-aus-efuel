# -*- coding: utf-8 -*-
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

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
        "H2 Electrolysis": "Hydrogen from electrolysis",
        "grey H2": "Grey hydrogen",
        "blue H2": "Blue hydrogen",
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
                "H2 Electrolysis",
                "grey H2",
                "blue H2",
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
