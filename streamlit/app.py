# -*- coding: utf-8 -*-
# SPDX-FileCopyrightText:  PyPSA-Earth and PyPSA-Eur Authors
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""
Streamlit application to interactively manage a PyPSA-Earth network,
adjust some key economic parameters, and run optimizations afterwards
to assess the impact of the adjustments on the network's costs.
"""

import os
import tempfile
from importlib.metadata import version

import altair as alt
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pypsa
import requests
from results_helpers import (
    compute_annual_flow_by_carrier,
    compute_capacity_by_carrier,
    compute_dispatch_annual_totals,
    compute_dispatch_by_carrier,
    get_available_dispatch_categories,
    get_available_result_categories,
)

import streamlit as st


def annuity_factor(discount_rate: float, lifetime: int) -> float:
    return discount_rate / (1 - (1 + discount_rate) ** -lifetime)


def investment_cost(
    annuity_payment: float, discount_rate: float, lifetime: int
) -> float:
    if discount_rate > 0:
        inv_cost = annuity_payment * (
            discount_rate / (1 - (1 + discount_rate) ** -lifetime)
        )
    else:
        inv_cost = annuity_payment * lifetime
    return inv_cost


# read current values and provide default values if non exist yet
default_dr = 7.0
default_om = 3.0  # %
# "lt": lifetime; "cc": capital_cost; "mc": marginal_cost; "dr": discount_rate; "label": label for UI
tech_data: dict[str, dict[str, int | float | str]] = {
    "solar rooftop": {
        "lt": 40,
        "cc": investment_cost(153711.113765, 0.044, 35),
        "fixom": 0.013,
        "mc": 1,
        "dr": 4.62,
        "label": "Solar PV Rooftop",
    },
    "solar": {
        "lt": 40,
        "cc": investment_cost(127897.547320, 0.044, 35),
        "fixom": 0.0151,
        "mc": 1,
        "dr": 4.38,
        "label": "Solar PV",
    },
    "onwind": {
        "lt": 30,
        "cc": investment_cost(844078.4, 0.07, 27),
        "fixom": 0.0208,
        "mc": 2,
        "dr": 5.19,
        "label": "Onshore Wind",
    },
    "offwind-ac": {
        "lt": 40,
        "cc": investment_cost(931643.3, 0.07, 20),
        "fixom": 0.025,
        "mc": 4,
        "dr": default_dr,
        "label": "Offshore Wind (AC)",
    },
    "offwind-dc": {
        "lt": 40,
        "cc": investment_cost(880935.4564626515, 0.07, 20),
        "fixom": 0.025,
        "mc": 6,
        "dr": default_dr,
        "label": "Offshore Wind (DC)",
    },
    "electrolysis": {
        "lt": 25,
        "cc": investment_cost(392818.710016, 0.07, 20),
        "fixom": 0.04,
        "mc": 1,
        "dr": default_dr,
        "label": "Electrolysis",
    },
}

MWH_PER_TONNE: dict[str, float] = {
    "diesel": 11.9,
    "custom_h2": 33.0,
    "grey_ammonia": 5.17,
    "e_ammonia": 5.17,
    "grey_methanol": 5.54,
    "e_methanol": 5.54,
}
KG_PER_LITER_DIESEL = 0.85

load_data: dict[str, dict[str, int | float | str | list[str]]] = {
    "custom_h2": {
        "multiplier": 1,
        "label": "Hydrogen",
        "cost": 2000,
        "carriers": [],
        "loads": ["custom H2 demand"],
    },
    "grey_ammonia": {
        "multiplier": 1,
        "label": "Grey ammonia",
        "cost": 700,
        "carriers": ["grey-ammonia"],
        "loads": [],
    },
    "e_ammonia": {
        "multiplier": 1,
        "label": "e-ammonia",
        "cost": 700,
        "carriers": ["e-ammonia"],
        "loads": [],
    },
    "grey_methanol": {
        "multiplier": 1,
        "label": "Grey methanol",
        "cost": 700,
        "carriers": ["grey-methanol"],
        "loads": [],
    },
    "e_methanol": {
        "multiplier": 1,
        "label": "e-methanol",
        "cost": 700,
        "carriers": ["e-methanol"],
        "loads": [],
    },
}


# -------------------- Helper functions --------------------
def get_snapshots(
    network: pypsa.Network,
    start_day: int = 1,
    end_day: int = 2,
    months: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
) -> np.ndarray:
    sns_all = network.snapshots
    periodic_index = sns_all[
        (sns_all.strftime("%d").astype(int).isin(range(start_day, end_day)))
        & (sns_all.strftime("%m").astype(int).isin(months))
    ]
    return periodic_index


def replace_nan(x: float, def_value: int = 0):
    return x if not np.isnan(x) else def_value


def round_multiple(number: float, multiple: float = 50.0):
    return float(multiple * round(number / multiple))


def get_loads_for_demand_entry(
    network: pypsa.Network,
    carriers: list[str],
    loads: list[str],
) -> pd.Index:
    """Return loads matching explicit load names or exact carrier names."""
    selected = pd.Index([])

    if loads:
        selected = selected.union(pd.Index(loads).intersection(network.loads.index))

    if carriers:
        selected = selected.union(
            network.loads.index[network.loads.carrier.isin(carriers)]
        )

    return selected


def to_fraction_discount_rate(discount_rate: float) -> float:
    """Convert discount rates above 1 from percent to fraction."""
    if pd.isna(discount_rate):
        return np.nan

    return discount_rate / 100 if discount_rate > 1 else discount_rate


def show_statistics(n: pypsa.Network):
    if st.session_state.n is not None:
        st.header("Network Statistics (rows)")
        st.write(f"Snapshots: {len(n.snapshots)}")
        comps = {}

        for c in n.components.keys() - ["Network", "SubNetwork"]:
            if len(getattr(n, n.components[c]["list_name"])):
                comps[c] = len(getattr(n, n.components[c]["list_name"]))

        df = pd.DataFrame.from_dict(comps, orient="index", columns=["Rows"])
        # don't show details about Global Constraints and Component Types
        df = df[~df.index.str.endswith("Constraint")]
        df = df[~df.index.str.endswith("Type")]
        st.bar_chart(df, height=275)
    return


def compact_number_tag(value: float, decimals: int = 1) -> str:
    """Return a compact numeric tag for scenario IDs."""
    return f"{value:.{decimals}f}".replace(".", "p")


def get_current_demand_values() -> dict[str, float]:
    """Return current demand values from the Streamlit session state in Mtpa."""
    old_multiplier = st.session_state.get("old_multiplier")
    new_multiplier = st.session_state.get("new_multiplier")

    source = new_multiplier if new_multiplier is not None else old_multiplier

    values = {
        "custom_h2": 0.0,
        "grey_ammonia": 0.0,
        "e_ammonia": 0.0,
        "grey_methanol": 0.0,
        "e_methanol": 0.0,
    }

    if source is None:
        return values

    for key in values:
        values[key] = float(source.get(key, 0.0))

    return values


def build_scenario_id(
    country: str = "AU",
    year: int = 2030,
    clusters: int = 10,
    resolution: str = "3h",
) -> str:
    """Build a deterministic scenario ID from current UI settings."""
    demand = get_current_demand_values()

    cost_tag = "costCustom" if st.session_state.get("costs_modified") else "costRef"

    return "_".join(
        [
            country,
            str(year),
            f"{clusters}",
            resolution,
            cost_tag,
            f"H2_{compact_number_tag(demand['custom_h2'])}Mt",
            f"gNH3_{compact_number_tag(demand['grey_ammonia'])}Mt",
            f"eNH3_{compact_number_tag(demand['e_ammonia'])}Mt",
            f"gMeOH_{compact_number_tag(demand['grey_methanol'])}Mt",
            f"eMeOH_{compact_number_tag(demand['e_methanol'])}Mt",
        ]
    )


def build_scenario_summary(
    country_name: str = "Australia",
    year: int = 2030,
    clusters: int = 10,
    resolution: str = "3h",
) -> str:
    """Build a human-readable scenario summary."""
    demand = get_current_demand_values()

    cost_label = (
        "Custom costs" if st.session_state.get("costs_modified") else "Reference costs"
    )

    ammonia = demand["grey_ammonia"] + demand["e_ammonia"]
    methanol = demand["grey_methanol"] + demand["e_methanol"]

    return " | ".join(
        [
            country_name,
            str(year),
            f"{clusters} clusters",
            resolution,
            cost_label,
            f"H2: {demand['custom_h2']:.1f} Mtpa",
            f"Grey ammonia: {demand['grey_ammonia']:.1f} Mtpa",
            f"e-ammonia: {demand['e_ammonia']:.1f} Mtpa",
            f"Grey methanol: {demand['grey_methanol']:.1f} Mtpa",
            f"e-methanol: {demand['e_methanol']:.1f} Mtpa",
        ]
    )


title = "AUS eFuels"
st.set_page_config(page_title=f"{title} UI", layout="wide")
st.title(f"{title} Interactive Manager")
st.write("Walk through the tabs below from left to the right ...")
with st.popover("Disclaimer", width="stretch", icon="⚠️"):
    st.write(
        """
        The content of this document/web page is intended for the exclusive use of **Open Energy Transition**'s client and other contractually agreed recipients. It may only be made available in whole or in part to third parties with the client’s consent and on a non-reliance basis. **Open Energy Transition** is not liable to third parties for the completeness and accuracy of the information provided therein.
        """
    )

if "n" not in st.session_state:
    st.session_state.n = None
if "opt_runs" not in st.session_state:
    st.session_state.opt_runs = 0
if "network_loaded" not in st.session_state:
    st.session_state.network_loaded = False
if "results" not in st.session_state:
    st.session_state.results = None
if "dr" not in st.session_state:
    st.session_state.dr = default_dr
if "old_multiplier" not in st.session_state:
    st.session_state.old_multiplier = None
if "new_multiplier" not in st.session_state:
    st.session_state.new_multiplier = None
if "new_cost" not in st.session_state:
    st.session_state.new_cost = None
if "PYPSA_VERSION" not in st.session_state:
    st.session_state.PYPSA_VERSION = None
if "costs_modified" not in st.session_state:
    st.session_state.costs_modified = False
if "solved_networks" not in st.session_state:
    st.session_state.solved_networks = {}
if "scenario_metadata" not in st.session_state:
    st.session_state.scenario_metadata = {}


# --- SIDEBAR ---
with st.sidebar:
    st.sidebar.header("Networks")

    with st.expander("Default PyPSA Network", expanded=True):
        zenodo_record_id = st.text_input("Zenodo Record ID", "20049009", disabled=True)
        zenodo_file_name = st.text_input(
            "File Name",
            "elec_s_10_ec_lv1_Co2L-3h_3h_2030_0.071_AB_0export.nc",
            disabled=True,
        )

        if st.button("Download"):
            api_url = f"https://zenodo.org/api/records/{zenodo_record_id}"
            res = requests.get(api_url).json()
            file_info = next(
                (f for f in res["files"] if f["key"] == zenodo_file_name), None
            )
            if file_info:
                SAVE_DIR = "./data"
                if not os.path.exists(SAVE_DIR):
                    os.makedirs(SAVE_DIR)
                download_url = file_info["links"]["self"]
                file_data = requests.get(download_url).content
                save_path = os.path.join(SAVE_DIR, zenodo_file_name)
                with requests.get(download_url, stream=True) as r:
                    r.raise_for_status()
                    with open(save_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)

                tmp_path = f"{SAVE_DIR}/{zenodo_file_name}"
                n = pypsa.Network(tmp_path)
                g = n.generators
                if "discount_rate" not in g.columns:
                    g["discount_rate"] = st.session_state.dr / 100
                else:
                    g["discount_rate"] = g["discount_rate"].apply(
                        to_fraction_discount_rate
                    )
                st.session_state.n = n
                st.session_state.costs_modified = False
                st.session_state.network_loaded = True
                st.success("Network loaded successfully!")

                # Cleanup the temp file
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            else:
                st.error("File not found in the given Zenodo record.")

    with st.expander("Local PyPSA-AUS Network", expanded=False):
        uploaded_file = st.file_uploader(
            "Choose a PyPSA NetCDF file", type=["nc"], max_upload_size=5  # 5 MB limit
        )

        if uploaded_file is not None and st.session_state.network_loaded is False:
            # PyPSA needs a file path, so we save the uploaded bytes to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".nc") as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                tmp_path = tmp_file.name

            # 2. Load the Network
            with st.spinner("Loading network..."):
                n = pypsa.Network(tmp_path)
                g = n.generators
                if "discount_rate" not in g.columns:
                    g["discount_rate"] = st.session_state.dr / 100
                else:
                    g["discount_rate"] = g["discount_rate"].apply(
                        to_fraction_discount_rate
                    )
                st.session_state.n = n
                st.session_state.costs_modified = False
                st.session_state.network_loaded = True
                st.success("Network loaded successfully!")

            # Cleanup the temp file
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    if st.session_state.network_loaded:
        show_statistics(st.session_state.n)

    st.write("---")
    pkgs = {}
    for pkg in ["highspy", "linopy", "pypsa", "streamlit"]:
        pkg_version = version(pkg)
        pkgs[pkg] = pkg_version
        if pkg == "pypsa":
            st.session_state.PYPSA_VERSION = version(pkg)

    df = pd.DataFrame.from_dict(pkgs, orient="index", columns=["Installed Versions"])
    st.dataframe(df)

# --- Tabs
tabs = [
    "| 👋 Welcome",
    "| 1. 💰 Economics",
    "| 2. 📊 Demands",
    "| 3. ⚡ Optimization",
    "| 4. 📈 Results",
]
t_welcome, t_economic, t_demand, t_optimization, t_results = st.tabs(
    tabs, on_change="rerun"
)

# --- TAB WELCOME
if t_welcome.open:
    with t_welcome:
        st.subheader("Welcome to the PyPSA-AUS-eFuels Interactive Manager!")
        st.write(
            """
            Use the sidebar to load your network and set project targets. Then, navigate through the tabs to manage different aspects of your project (economic and demand parameters).

            In 2025, Australia's **Diesel** consumption was about 32 bn liter or 27.2 Mtpa (million ton per annum). About 85% (or more than 23 Mtpa) needed to be imported. Assuming AUD 3/liter, this makes more than AUD ~80 bn of import costs every year. A historical growth rate of 5-10%/year has been observed and is expected going forward.
            **Ammonia** consumption was about 2 Mtpa, where about 50% was used in agriculture, 35% in mining and explosives, and the rest in industry and chemicals.
            While short distance truck transport and a significant share of mining might be replaced by electric vehicles, long distance transport via truck and train likely still rely on liquid fuels for the foreseeable future.

            **10% (~2.3 Mtpa or 2.7 million liters) diesel replacement would save AUD ~8 bn per year in import costs, which could be used to invest in local production, local renewable energy and local employment instead.**
            """
        )
        with st.popover("Project Description", width="stretch", icon="📄"):
            st.write(
                """
                This application has been developed during a project between **Open Energy Transition** and **Sagax Capital / Keshik Capital** to assess the impact on Australia on local Ammonia and Methanol production.

                The project aims to evaluate the potential for local production of these chemicals using renewable energy sources, and how this does help Australia in its energy transition and resilience.

                **The entire project source is available on GitHub: https://github.com/open-energy-transition/pypsa-aus-efuel.**
                """
            )

# --- TAB ECONOMIC PARAMETERS
if t_economic.open:
    with t_economic:
        if st.session_state.n is None:
            st.info("Please load a network ...")
            st.write(
                "After loading a network, you are able to adjust the economic parameters."
            )
        else:
            st.header("Economic Parameters")

            n = st.session_state.n
            g = n.generators
            with st.expander("Selected Economic Parameters", expanded=True):
                st.write(
                    "Choose Capital Cost and Marginal Cost to be used for your case:"
                )
                old_lt = {}
                old_dr = {}
                old_ui_dr = {}
                new_dr = {}
                old_cc = {}
                old_ui_cc = {}
                new_cc = {}
                old_mc = {}
                old_ui_mc = {}
                new_mc = {}
                # Discount rates are stored in the PyPSA network as fractions
                # (e.g. 0.07 for 7%), while the Streamlit UI displays percentages.
                for d in tech_data:
                    old_lt[d] = replace_nan(
                        g.loc[g.carrier.str.startswith(d), "lifetime"].mean(),
                        tech_data[d]["lt"],
                    )
                    old_dr_fraction = replace_nan(
                        g.loc[g.carrier.str.startswith(d), "discount_rate"].mean(),
                        tech_data[d]["dr"] / 100,
                    )
                    old_dr[d] = old_dr_fraction * 100
                    old_cc[d] = replace_nan(
                        g.loc[g.carrier.str.startswith(d), "capital_cost"].mean(),
                        investment_cost(tech_data[d]["cc"], old_dr[d], old_lt[d]),
                    )
                    old_mc[d] = replace_nan(
                        g.loc[g.carrier.str.startswith(d), "marginal_cost"].mean(),
                        tech_data[d]["mc"],
                    )

                col1, col2, col3, col4 = st.columns(4, vertical_alignment="top")
                col2.write("**Discount Rate (%)**")
                col3.write("**Overnight Investment Cost (AUD/MW)**")
                col4.write("**Marginal Cost (AUD/MWh)**")

                for d in tech_data:
                    col1, col2, col3, col4 = st.columns(4, vertical_alignment="top")
                    col1.write(f"**{tech_data[d]['label']}**")

                    with col2:
                        old_ui_dr[d] = round_multiple(old_dr[d], 0.1)

                        new_dr[d] = st.slider(
                            label=f"dr_{tech_data[d]['label']}",
                            label_visibility="collapsed",
                            min_value=0.1,
                            max_value=20.0,
                            value=old_ui_dr[d],
                            step=0.1,
                            format="%.1f%%",
                        )

                    with col3:
                        initial_cc = investment_cost(old_cc[d], new_dr[d], old_lt[d])
                        st.session_state.setdefault(f"initial_cc_{d}", initial_cc)

                        old_ui_cc[d] = investment_cost(old_cc[d], new_dr[d], old_lt[d])

                        new_cc[d] = st.slider(
                            label=f"cc_{tech_data[d]['label']}",
                            label_visibility="collapsed",
                            min_value=1.0,
                            max_value=10_000_000.0,
                            value=old_ui_cc[d],
                            step=0.1,
                            format="%,.1f AUD/MW",
                        )

                    with col4:
                        old_ui_mc[d] = round_multiple(old_mc[d], 0.1)

                        new_mc[d] = st.slider(
                            label=f"mc_{tech_data[d]['label']}",
                            label_visibility="collapsed",
                            min_value=0.0,
                            max_value=20.0,
                            value=old_ui_mc[d],
                            step=0.1,
                            format="%.1f AUD/MWh",
                        )
                st.write(
                    f"Remark: It is assumed to have a fixed O&M with {default_om}%/year for each technology!"
                )

            if st.button("Apply New Costs"):
                g["discount_rate"] = g.get("discount_rate", st.session_state.dr)
                g.loc[g.discount_rate.isnull(), "discount_rate"] = (
                    st.session_state.dr / 100
                )

                for d in tech_data:
                    if len(g.carrier[g.carrier == d]):
                        # don't change 'lifetime' for now
                        g.loc[g.carrier.str.startswith(d), "discount_rate"] = (
                            new_dr[d] / 100
                        )
                        g.loc[g.carrier.str.startswith(d), "capital_cost"] = (
                            new_cc[d]
                            * annuity_factor(new_dr[d] / 100, tech_data[d]["lt"])
                            * (1 + default_om / 100)
                        )
                        g.loc[g.carrier.str.startswith(d), "marginal_cost"] = new_mc[d]
                        g.loc[g.carrier.str.startswith(d), "overnight_cost"] = new_cc[d]
                        g.loc[g.carrier.str.startswith(d), "fom_cost"] = (
                            new_cc[d] * default_om / 100
                        )

                st.session_state.costs_modified = any(
                    not np.isclose(new_dr[d], old_ui_dr[d])
                    or not np.isclose(new_cc[d], old_ui_cc[d])
                    or not np.isclose(new_mc[d], old_ui_mc[d])
                    for d in tech_data
                )
                st.success("Updated details for mentioned technologies ...")
                st.write(
                    "Remark: in this table the column capital_cost refersto annuity plus fixed O&M costs."
                )
                st.write(
                    g[
                        [
                            "capital_cost",
                            "marginal_cost",
                            "discount_rate",
                            "overnight_cost",
                            "fom_cost",
                        ]
                    ]
                )

# --- TAB DEMAND PARAMETERS
if t_demand.open:
    with t_demand:
        if st.session_state.n is None:
            st.info("Please load a network ...")
            st.write(
                "After loading a network, you are able to adjust the demand parameters."
            )
        else:
            st.header("Demand Parameters")
            n = st.session_state.n
            with st.expander("Selected Demand Parameters", expanded=True):
                st.write("Choose Load Multipliers to be used for your case:")
                old_multiplier = {}
                new_multiplier = {}
                # collect the current demand
                for l in load_data:
                    # get the loads associated with the current load, e.g., e-ammonia
                    loads = get_loads_for_demand_entry(
                        n,
                        carriers=load_data[l]["carriers"],
                        loads=load_data[l]["loads"],
                    )

                    # calculate the sum of the loads collected
                    if len(loads) == 0:
                        old_multiplier[l] = 0.0

                    elif l in MWH_PER_TONNE:
                        available_loads = loads.intersection(n.loads_t.p.columns)

                        if len(available_loads) > 0:
                            annual_mwh = (
                                n.loads_t.p[available_loads]
                                .multiply(n.snapshot_weightings.generators, axis=0)
                                .sum()
                                .sum()
                            )
                        else:
                            annual_mwh = (
                                n.loads.loc[loads, "p_set"].sum()
                                * n.snapshot_weightings.generators.sum()
                            )

                        old_multiplier[l] = annual_mwh / MWH_PER_TONNE[l] / 1e6

                    else:
                        old_multiplier[l] = 0.0

                if st.session_state.new_cost is None:
                    new_cost = {}
                    for l in load_data:
                        # get the current avoided price assumptions
                        new_cost[l] = load_data[l]["cost"]

                    st.session_state.new_cost = new_cost
                else:
                    new_cost = st.session_state.new_cost

                col1, col2, col3, col4 = st.columns(4, vertical_alignment="top")
                col2.write("**Current Demand**")
                col3.write("**New / Proposed Demand**")
                col4.write("**Avoided Import Price / Tonne**")

                for l in load_data:
                    col1, col2, col3, col4 = st.columns(4, vertical_alignment="top")

                    col1.write(f"**{load_data[l]['label']}**")
                    col2.write(f"{old_multiplier[l]:.1f} Mtpa")

                    with col3:
                        new_multiplier[l] = st.slider(
                            label=f"Demand Multiplier {l}",
                            label_visibility="collapsed",
                            min_value=0.0,
                            max_value=20.0,
                            step=0.1,
                            value=round_multiple(old_multiplier[l], 0.1),
                            format="%.1f Mtpa",
                        )

                    with col4:
                        new_cost[l] = st.slider(
                            label=f"Cost {l}",
                            label_visibility="collapsed",
                            min_value=0.0,
                            max_value=10_000.0,
                            step=0.1,
                            value=round_multiple(new_cost[l], 0.1),
                            format="%.1f AUD/t",
                        )

                        if l in ["grey_methanol", "e_methanol"]:
                            diesel_equivalent = (
                                new_cost[l]
                                * MWH_PER_TONNE["diesel"]
                                / MWH_PER_TONNE[l]
                                / 1000
                                / KG_PER_LITER_DIESEL
                            )

                            st.caption(
                                f"Equivalent Diesel Replacement Value: "
                                f"{diesel_equivalent:.2f} AUD/liter"
                            )

                st.session_state.old_multiplier = old_multiplier
                st.session_state.new_multiplier = new_multiplier
                st.session_state.new_cost = new_cost

            if st.button("Apply New Demand"):
                name_loads = []
                for l in load_data:
                    loads = get_loads_for_demand_entry(
                        n,
                        carriers=load_data[l]["carriers"],
                        loads=load_data[l]["loads"],
                    )
                    nr_loads = len(loads)

                    if nr_loads == 0:
                        continue

                    if l not in MWH_PER_TONNE:
                        continue

                    annual_mwh = new_multiplier[l] * 1e6 * MWH_PER_TONNE[l]
                    new_p_set = (
                        annual_mwh / n.snapshot_weightings.generators.sum() / nr_loads
                    )

                    for load in loads:
                        n.loads.loc[load, "p_set"] = new_p_set
                        n.loads_t.p[load] = new_p_set
                        name_loads.append(load)

                st.success("Updated details for mentioned carriers ...")
                df = n.loads[["carrier", "p_set"]]
                st.dataframe(df[df.index.isin(name_loads)], height=500)

# --- TAB OPTIMIZATION
if t_optimization.open:
    with t_optimization:
        if st.session_state.n is None:
            st.info("Please load a network ...")
            st.write("After loading a network, you are able to optimize the network.")
        else:
            n = st.session_state.n
            new_multiplier = st.session_state.new_multiplier
            new_cost = st.session_state.new_cost

            st.header("Run Optimization")

            scenario_id = build_scenario_id()
            scenario_summary = build_scenario_summary()
            demand = get_current_demand_values()

            ammonia = demand["grey_ammonia"] + demand["e_ammonia"]
            methanol = demand["grey_methanol"] + demand["e_methanol"]

            with st.expander("Scenario Overview", expanded=True):
                st.write("**Scenario ID**")
                st.code(scenario_id, language=None)

                st.write("**Scenario Summary**")
                st.write(scenario_summary)

            with st.expander("Configuration", expanded=True):
                col1, col2, col3, col4 = st.columns(4)

                col1.metric("Country", "Australia")
                col2.metric("Planning year", "2030")
                col3.metric("Clusters", "10")
                col4.metric("Resolution", "3h")

                col1, col2, col3, col4 = st.columns(4)

                cost_setup = (
                    "Custom" if st.session_state.get("costs_modified") else "Reference"
                )
                col1.metric("Cost setup", cost_setup)
                col2.metric("H2 demand", f"{demand['custom_h2']:.1f} Mtpa")
                col3.metric("Grey ammonia", f"{demand['grey_ammonia']:.1f} Mtpa")
                col4.metric("e-ammonia", f"{demand['e_ammonia']:.1f} Mtpa")

                col1, col2, col3, col4 = st.columns(4)

                col1.metric("Grey methanol", f"{demand['grey_methanol']:.1f} Mtpa")
                col2.metric("e-methanol", f"{demand['e_methanol']:.1f} Mtpa")
                col3.metric("Total ammonia", f"{ammonia:.1f} Mtpa")
                col4.metric("Total methanol", f"{methanol:.1f} Mtpa")

            with st.expander("Snapshot Options", expanded=True):
                col1, col2, col3 = st.columns(3, vertical_alignment="top")

                with col1:
                    run_mode = st.radio(
                        "Select desired optimization snapshots:",
                        ["Full Year", "Full Month", "Week per Month"],
                        index=2,
                        horizontal=True,
                    )

                with col2:
                    months = st.multiselect(
                        "Select months to consider:",
                        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        default=[1],
                    )

                with col3:
                    if run_mode == "Week per Month":
                        weeks = st.radio(
                            "Select week within selected months:",
                            [1, 2, 3, 4],
                            index=0,
                            horizontal=True,
                        )
                    else:
                        weeks = None

            with st.expander("Solver Options", expanded=True):
                solver_name = st.radio(
                    "Select the solver to use for optimization:",
                    ["highs", "OETC"],
                    index=0,
                    horizontal=True,
                )

            if st.button("Run LOPF"):
                n2 = n.copy()

                if run_mode in ["Full Month", "Week per Month"]:
                    sns_before = len(n2.snapshots)

                    if run_mode == "Full Month":
                        sns_subset = n2.snapshots[
                            n2.snapshots.strftime("%m").astype(int).isin(months)
                        ]

                    elif run_mode == "Week per Month":
                        start_day = (weeks - 1) * 7 + 1
                        end_day = start_day + 7

                        sns_subset = get_snapshots(
                            n2,
                            start_day=start_day,
                            end_day=end_day,
                            months=months,
                        )

                    sns_after = len(sns_subset)

                    if sns_after == 0:
                        st.error(
                            "No snapshots selected. Please choose at least one valid month/week."
                        )
                        st.stop()

                    n2.set_snapshots(sns_subset)
                    n2.snapshot_weightings = (
                        n2.snapshot_weightings * sns_before / sns_after
                    )

                st.info(f"Optimizing for {len(n2.snapshots)} snapshots ...")
                st.session_state.opt_runs += 1
                with st.spinner("Solving Network ..."):
                    n2.consistency_check()
                    if (
                        st.session_state.PYPSA_VERSION is not None
                        and st.session_state.PYPSA_VERSION > "1.0.0"
                    ):
                        n2.sanitize()

                    if solver_name == "OETC":
                        st.warning(
                            "The Open Energy Transition Cluster (OETC) is not configured yet. Therefore 'highs' is used."
                        )
                        solver_name = "highs"

                    status, condition = n2.optimize(
                        solver_name=solver_name,
                        assign_all_duals=False,
                        solver_options={
                            "solver": "hipo",
                            "user_objective_scale": -2,
                            "user_bound_scale": -14,
                        },
                    )

                if status == "ok":
                    st.success(f"Optimization finished: {condition}")

                    # calculate the annual costs for importing e-fuels otherwise
                    if new_cost is None or new_multiplier is None:
                        st.warning(
                            "Demand parameters were not applied. Import-cost comparison is skipped."
                        )
                        avoided_import_cost = None
                    else:
                        avoided_import_cost = 0.0

                        for l in load_data:
                            loads = get_loads_for_demand_entry(
                                n2,
                                carriers=load_data[l]["carriers"],
                                loads=load_data[l]["loads"],
                            )

                            if len(loads) == 0:
                                continue

                            avoided_import_cost += new_multiplier[l] * new_cost[l] * 1e6

                    optimized_system_cost = n2.objective
                    expanded_cap = n2.statistics.expanded_capacity().round(1)

                    expanded_cap[("Economics", "Annuity")] = round(
                        optimized_system_cost / 1e6, 1
                    )  # million AUD

                    if avoided_import_cost is not None:
                        expanded_cap[("Economics", "Savings")] = round(
                            (avoided_import_cost - optimized_system_cost) / 1e6, 1
                        )  # million AUD

                    run_name = scenario_id

                    if (
                        st.session_state.results is not None
                        and run_name in st.session_state.results.columns
                    ):
                        run_name = f"{scenario_id}_r{st.session_state.opt_runs}"

                    st.session_state.solved_networks[run_name] = n2
                    st.session_state.scenario_metadata[run_name] = scenario_summary

                    if st.session_state.results is None:
                        cap_df = expanded_cap.to_frame(name=run_name)
                    else:
                        cap_df = st.session_state.results.join(
                            expanded_cap.to_frame(name=run_name)
                        )

                    # save the cap_df to be used in the 'Results' tab
                    st.session_state.results = cap_df

                    st.write("Check the 'Results' tab for details.")
                else:
                    st.error(f"Solver failed: {condition}")

# ---TAB RESULTS
if t_results.open:
    with t_results:
        # if st.session_state.results is not None:
        if len(st.session_state.solved_networks) > 0:
            st.header("Results Explorer")

            available_runs = list(st.session_state.solved_networks.keys())

            selected_runs = st.multiselect(
                "Select solved scenarios",
                available_runs,
                default=available_runs,  # [-1:],
                width="stretch",
            )

            result_view = st.radio(
                "Select result view",
                ["Installed capacity", "Dispatch"],
                horizontal=True,
            )

            if selected_runs:
                selected_networks = {
                    run: st.session_state.solved_networks[run] for run in selected_runs
                }

                if result_view == "Installed capacity":
                    category = st.radio(
                        "Select result category",
                        get_available_result_categories(),
                        horizontal=True,
                    )

                    if category == "Electricity":
                        cap_df = compute_capacity_by_carrier(
                            selected_networks, category
                        )
                        y_label = "GW"
                        result_title = "Electricity - Installed / Expanded Capacity"
                    else:
                        cap_df = compute_annual_flow_by_carrier(
                            selected_networks,
                            category,
                            MWH_PER_TONNE,
                        )
                        y_label = "Mtpa"
                        result_title = f"{category} - Annual Production / Capture"

                    st.subheader(result_title)

                    if cap_df.empty:
                        st.warning(f"No result data found for {category}.")
                    else:
                        chart_df = cap_df.pivot_table(
                            index="scenario",
                            columns="carrier",
                            values="value",
                            aggfunc="sum",
                            fill_value=0.0,
                        )

                        st.bar_chart(chart_df, y_label=y_label, height=600)

                        table_df = (
                            cap_df.drop(columns=["component"], errors="ignore")
                            .pivot_table(
                                index=["carrier", "unit"],
                                columns="scenario",
                                values="value",
                                aggfunc="sum",
                                fill_value=0.0,
                            )
                            .reset_index()
                            .rename(columns={"carrier": "Carrier", "unit": "Unit"})
                        )

                        with st.expander(f"Show {category} data table", expanded=False):
                            st.dataframe(
                                table_df,
                                width="stretch",
                                hide_index=True,
                            )

                elif result_view == "Dispatch":
                    dispatch_category = st.radio(
                        "Select dispatch category",
                        get_available_dispatch_categories(),
                        horizontal=True,
                    )

                    dispatch_run = st.selectbox(
                        "Select scenario for dispatch",
                        selected_runs,
                        index=0,
                    )

                    n_dispatch = st.session_state.solved_networks[dispatch_run]

                    dispatch_df = compute_dispatch_by_carrier(
                        n_dispatch,
                        dispatch_category,
                    )

                    y_label = "GW" if dispatch_category == "Electricity" else "kt"

                    st.subheader(f"{dispatch_category} - Dispatch")
                    st.caption(f"Scenario: {dispatch_run}")

                    if dispatch_df.empty:
                        st.warning(f"No dispatch data found for {dispatch_category}.")
                    else:
                        plot_df = dispatch_df.reset_index().melt(
                            id_vars=dispatch_df.index.name or "index",
                            var_name="Technology",
                            value_name="Value",
                        )

                        time_col = dispatch_df.index.name or "index"

                        chart = (
                            alt.Chart(plot_df)
                            .mark_area()
                            .encode(
                                x=alt.X(f"{time_col}:T", title="Snapshot"),
                                y=alt.Y("Value:Q", stack="zero", title=y_label),
                                color=alt.Color("Technology:N", title="Technology"),
                                tooltip=[
                                    alt.Tooltip(f"{time_col}:T", title="Snapshot"),
                                    alt.Tooltip("Technology:N"),
                                    alt.Tooltip("Value:Q", format=",.2f"),
                                ],
                            )
                            .properties(height=600)
                        )

                        st.altair_chart(chart, width="stretch")

                        annual_table = compute_dispatch_annual_totals(
                            n_dispatch,
                            dispatch_df,
                            dispatch_category,
                        )

                        with st.expander(
                            f"Show {dispatch_category} annual totals",
                            expanded=False,
                        ):
                            st.dataframe(
                                annual_table,
                                width="stretch",
                                hide_index=True,
                            )

                st.header("Technical Comparison")
                st.write(
                    "Only the technologies being different are shown in the table below, while the economic comparison is shown in the chart below."
                )
                df = st.session_state.results
                # don't show economic details in the technical comparison
                df = df[~df.index.get_level_values(0).str.contains("Economics")]
                df = df[df.index.get_level_values(0).str.contains("Link")]
                # only show rows where there is a difference in the values across runs
                df = df[df.nunique(axis=1) > 1].T
                st.dataframe(df.T.style.format("{:.1f}"))
                if "scenario_metadata" in st.session_state:
                    st.subheader("Scenario Descriptions")
                    st.write(
                        """
                        Below you can find the descriptions for each optimized scenario.
                        Demand for hydrogen and hydrogen-based derivatives are given in Mtpa (million ton per annum).
                        """
                    )

                    df = pd.DataFrame(
                        columns=[
                            "Run",
                            "Country",
                            "Year",
                            "Clusters",
                            "Resolution",
                            "Cost Setup",
                            "H2 Demand",
                            "Grey Ammonia",
                            "e-Ammonia",
                            "Grey Methanol",
                            "e-Methanol",
                        ]
                    )
                    run_nr = 1
                    for k, v in st.session_state.scenario_metadata.items():
                        (
                            col1,
                            col2,
                            col3,
                            col4,
                            col5,
                            col6,
                            col7,
                            col8,
                            col9,
                            col10,
                            col11,
                        ) = st.columns(11, vertical_alignment="top")
                        v_split = v.split("|")
                        st.markdown(f"- **Run {run_nr}**: {k}")
                        new_row = {
                            "Run": run_nr,
                            "Country": v_split[0],
                            "Year": v_split[1],
                            "Clusters": v_split[2].replace("clusters", ""),
                            "Resolution": v_split[3],
                            "Cost Setup": v_split[4],
                            "H2 Demand": v_split[5]
                            .replace("Mtpa", "")
                            .replace("H2: ", ""),
                            "Grey Ammonia": v_split[6]
                            .replace("Mtpa", "")
                            .replace("Grey ammonia: ", ""),
                            "e-Ammonia": v_split[7]
                            .replace("Mtpa", "")
                            .replace("e-ammonia: ", ""),
                            "Grey Methanol": v_split[8]
                            .replace("Mtpa", "")
                            .replace("Grey methanol: ", ""),
                            "e-Methanol": v_split[9]
                            .replace("Mtpa", "")
                            .replace("e-methanol: ", ""),
                        }
                        df.loc[len(df)] = new_row
                        run_nr += 1

                    st.dataframe(df, hide_index=True)

                st.header("Economic Comparison")
                df = st.session_state.results
                df = df / 1e3  # convert to million AUD
                # only show economic details
                df = df[df.index.get_level_values(0).str.contains("Economics")].round(1)
                df = df.reset_index().drop(columns=["component"])
                df = df.set_index("carrier")
                st.bar_chart(
                    df.T,
                    x_label="Runs",
                    y_label="Annual Cost (Million AUD)",
                    horizontal=True,
                )
        else:
            st.info(
                "Please load a network and run an optimization to see results here ..."
            )
            st.write(
                """
                After running an optimization, you will see a detailed breakdown of the expanded capacities and economic outcomes for each technology, allowing you to assess the impact of your parameter adjustments on the network's performance and costs.
                """
            )
