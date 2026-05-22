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
from pathlib import Path

import altair as alt
import geopandas as gpd
import numpy as np
import pandas as pd
import pypsa
import requests
from results_helpers import *

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
default_om = 3.0
# "lt": lifetime; "cc": capital_cost; "mc": marginal_cost; "dr": discount_rate; "label": label for UI
tech_data: dict[str, dict[str, int | float | str]] = {
    "solar rooftop": {
        "lt": 40,
        "cc": investment_cost(153711.113765, 0.044, 35),
        "fixom": 0.013,
        "mc": 1,
        "dr": default_dr,
        "label": "Solar PV Rooftop",
    },
    "solar": {
        "lt": 40,
        "cc": investment_cost(127897.547320, 0.044, 35),
        "fixom": 0.0151,
        "mc": 1,
        "dr": default_dr,
        "label": "Solar PV",
    },
    "onwind": {
        "lt": 30,
        "cc": investment_cost(844078.4, 0.07, 27),
        "fixom": 0.0208,
        "mc": 2,
        "dr": default_dr,
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

ELECTROLYZER_LINK_CARRIERS = [
    "Alkaline electrolyzer small",
    "Alkaline electrolyzer medium",
    "Alkaline electrolyzer large",
    "SOEC",
    "PEM electrolyzer",
]

MWH_PER_TONNE: dict[str, float] = {
    "diesel": 11.9,
    "custom_h2": 33.0,
    "grey_ammonia": 5.17,
    "e_ammonia": 5.17,
    "grey_methanol": 5.54,
    "e_methanol": 5.54,
}
KG_PER_LITER_DIESEL = 0.85
T_PER_GJ_DIESEL = 42.8  # or MT per PJ
DEFAULT_E_SHARE = 0.50
DEFAULT_E_SHARE_PRODUCTION = 0.30

# ----- diesel / methanol demand
# source: Department of Climate Change, Energy, the Environment and Water, Australian Energy Statistics, Table F, August 2025
# last available data for 2023-24
sectors: dict[str, float] = {
    "Mining": {"demand": 299.0 / T_PER_GJ_DIESEL, "e-share": DEFAULT_E_SHARE},
    "Transport": {"demand": 765.2 / T_PER_GJ_DIESEL, "e-share": DEFAULT_E_SHARE},
    "Agriculture": {"demand": 88.8 / T_PER_GJ_DIESEL, "e-share": DEFAULT_E_SHARE},
    "Manufacturing": {"demand": 13.9 / T_PER_GJ_DIESEL, "e-share": DEFAULT_E_SHARE},
    "Construction": {"demand": 26.5 / T_PER_GJ_DIESEL, "e-share": DEFAULT_E_SHARE},
    "Commercial Services": {
        "demand": 32.1 / T_PER_GJ_DIESEL,
        "e-share": DEFAULT_E_SHARE,
    },
}

# ----- fertlizer demand
fertilizeres: dict[str, float] = {
    "Urea": {
        "demand": 3.8,
        "ammonia_equiv": 0.57,
        "e-share": 0.00,
    },
    "Ammonia": {
        "demand": 0.1,
        "ammonia_equiv": 1.00,
        "e-share": 0.00,
    },
    "MAP": {
        "demand": 0.7,
        "ammonia_equiv": 0.15,
        "e-share": 0.00,
    },
    "DAP": {
        "demand": 0.6,
        "ammonia_equiv": 0.26,
        "e-share": 0.00,
    },
}

DISPATCH_COLORS = {
    "Utility solar": "#f9d002",
    "Rooftop solar": "#ffea80",
    "Onshore wind": "#235ebc",
    "Offshore wind AC": "#6895dd",
    "Offshore wind DC": "#74c6f2",
    "Run-of-river hydro": "#3dbfb0",
    "Hydro": "#298c81",
    "Pumped hydro": "#51dbcc",
    "Battery": "#b88300",
    "Biomass": "#4fba41",
    "Coal": "#000000",
    "Oil": "#555555",
    "Gas OCGT": "#db6a00",
    "Gas CCGT": "#db0000",
    "Grey ammonia": "#b100ff",
    "e-ammonia": "#e5abff",
    "Grey methanol": "#ed0202",
    "e-methanol": "#ff8080",
    "SMR": "#666666",
    "SMR CC": "#0059ff",
    "DAC": "#eea3ff",
    "PEM electrolyzer": "#2ecbff",
    "SOEC": "#f5ff2e",
    "Alkaline electrolyzer large": "#1b9e77",
    "Alkaline electrolyzer medium": "#66c2a5",
    "Alkaline electrolyzer small": "#b2df8a",
}

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
        "cost": 1000,
        "carriers": ["e-methanol"],
        "loads": [],
    },
}


# Helper functions
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
    return x if pd.notna(x) and np.isfinite(x) else def_value


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
    st.write("""
        The content of this document/web page is intended for the exclusive use of **Open Energy Transition**'s client and other contractually agreed recipients. It may only be made available in whole or in part to third parties with the client’s consent and on a non-reliance basis. **Open Energy Transition** is not liable to third parties for the completeness and accuracy of the information provided therein.
        """)

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
if "scenario_labels" not in st.session_state:
    st.session_state.scenario_labels = {}
if "new_demand_meoh" not in st.session_state:
    st.session_state.new_demand_meoh = None
if "new_demand_nh3" not in st.session_state:
    st.session_state.new_demand_nh3 = None

# SIDEBAR
with st.sidebar:
    st.sidebar.header("Networks")

    def normalize_generator_discount_rates(n: pypsa.Network) -> None:
        """Ensure generator discount rates exist and are stored as fractions."""
        g = n.generators

        if "discount_rate" not in g.columns:
            g["discount_rate"] = st.session_state.dr / 100
        else:
            g["discount_rate"] = g["discount_rate"].apply(to_fraction_discount_rate)

    def register_loaded_network(n: pypsa.Network) -> None:
        """Store a loaded network in Streamlit session state."""
        normalize_generator_discount_rates(n)

        st.session_state.n = n
        st.session_state.costs_modified = False
        st.session_state.network_loaded = True
        st.success("Network loaded successfully!")

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
                tmp_path = os.path.join(SAVE_DIR, zenodo_file_name)

                with requests.get(download_url, stream=True) as r:
                    r.raise_for_status()
                    with open(tmp_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)

                n = pypsa.Network(tmp_path)
                register_loaded_network(n)

                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            else:
                st.error("File not found in the given Zenodo record.")

    with st.expander("Local PyPSA-AUS Network", expanded=False):
        uploaded_file = st.file_uploader(
            "Choose a PyPSA NetCDF file",
            type=["nc"],
            max_upload_size=5,
        )

        if "uploaded_network_name" not in st.session_state:
            st.session_state.uploaded_network_name = None

        if uploaded_file is not None:
            if uploaded_file.name != st.session_state.uploaded_network_name:
                with tempfile.NamedTemporaryFile(
                    delete=False, suffix=".nc"
                ) as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    tmp_path = tmp_file.name

                with st.spinner("Loading network..."):
                    n = pypsa.Network(tmp_path)
                    register_loaded_network(n)
                    st.session_state.uploaded_network_name = uploaded_file.name

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

# Tabs
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

# TAB WELCOME
if t_welcome.open:
    with t_welcome:
        st.subheader("Welcome to the PyPSA-AUS-eFuels Interactive Manager!")
        st.write(f"""
            Use the sidebar to load your network and set project targets. Then, navigate through the tabs to manage different aspects of your project (economic and demand parameters).

            By default it is assumed that {DEFAULT_E_SHARE*100}% of the diesel demand can be reduced by electrification.
            Additionally it is assumed that {DEFAULT_E_SHARE_PRODUCTION*100}% of the remaining diesel and ammonia demand shall be covered by local green production.
            To review and/or adjust the required methanol and/or ammonia demand settings pull down the relevant pull-down box.
            The calculated e-methanol and e-ammonia production values are automatically transferred to the “Demand Parameters” tab, where they can still be manually adjusted before being applied to the network.
            """)

        # ----- sectors
        with st.expander(
            "Detailed Demand Split Parameters for Diesel / Methanol", expanded=False
        ):

            col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(
                8, vertical_alignment="top"
            )
            col1.write("**Sector**")
            col2.write("**Historic Diesel Demand (Mtpa)**")
            col3.write("**Electrified Demand Share (%)**")
            col4.write("**Remaining Diesel Demand (Mtpa)**")
            col5.write("**Domestic Grey Diesel Supply (Mtpa)**")
            col6.write("**Domestic Grey Diesel Share (%)**")
            col7.write("**Requested e-Diesel Share (%)**")
            col8.write("**Required e-Methanol Production (Mtpa)**")

            old_demand = {}
            new_demand_meoh = {}
            new_share = {}
            total_demand = 0
            total_remaining_demand = 0
            for s in sectors:
                old_demand[s] = sectors[s]["demand"]
                col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(
                    8, vertical_alignment="top"
                )
                col1.write(f"**{s}**")
                col2.write(f"{sectors[s]['demand']:.1f} ")
                with col3:
                    new_share[s] = st.slider(
                        label=f"Electrification Share {s}",
                        label_visibility="collapsed",
                        min_value=0.0,
                        max_value=100.0,
                        step=1.0,
                        value=sectors[s]["e-share"] * 100,
                        format="%.0f%%",
                    )

                new_demand_meoh[s] = old_demand[s] * (1 - new_share[s] / 100)
                col4.write(f"{new_demand_meoh[s]:.1f}")

                total_demand += sectors[s]["demand"]
                total_remaining_demand += new_demand_meoh[s]

            col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(
                8, vertical_alignment="top"
            )
            col1.write("**Total**")
            col2.write(f"{total_demand:.1f}")
            total_electrification_share = (
                total_demand - total_remaining_demand
            ) / total_demand
            col3.slider(
                label=f"Electrification Share {s}",
                label_visibility="collapsed",
                min_value=0.0,
                max_value=100.0,
                step=0.1,
                value=total_electrification_share * 100,
                format="%.1f%%",
                disabled=True,
            )
            col4.write(f"{total_remaining_demand:.1f}")

            with col5:
                domestic_supply = st.slider(
                    label="Domestic Diesel Supply",
                    label_visibility="collapsed",
                    min_value=0.0,
                    max_value=total_remaining_demand,
                    step=0.1,
                    value=4.5,
                    format="%.1f Mtpa",
                )
            with col6:
                domestic_supply_share = st.slider(
                    label="Domestic Diesel Share",
                    label_visibility="collapsed",
                    min_value=0.0,
                    max_value=100.0,
                    step=0.1,
                    value=domestic_supply / total_remaining_demand * 100,
                    format="%.1f%%",
                    disabled=True,
                )
            with col7:
                domestic_requested_share = st.slider(
                    label="Requested Diesel e-Share",
                    label_visibility="collapsed",
                    min_value=0.0,
                    max_value=100.0,
                    step=1.0,
                    value=DEFAULT_E_SHARE_PRODUCTION * 100,
                    format="%.0f%%",
                )
            with col8:
                domestic_requested_demand = st.slider(
                    label="Methanol Demand",
                    label_visibility="collapsed",
                    min_value=0.0,
                    max_value=total_remaining_demand,
                    step=0.1,
                    value=(domestic_requested_share)
                    / 100
                    * (total_remaining_demand - domestic_supply),
                    format="%.1f Mtpa",
                    disabled=True,
                )

            st.session_state.new_demand_meoh = domestic_requested_demand

            st.write(
                "**Source**: *Department of Climate Change, Energy, the Environment and Water, Australian Energy Statistics, Table F, August 2025 (Demand numbers 2023-24).*"
            )

        st.write(
            f"**Considered local e-Methanol production: {st.session_state.new_demand_meoh:.1f} Mtpa**"
        )

        # ----- fertilizeres
        with st.expander(
            "Detailed Demand Split Parameters for Fertilizers / Ammonia", expanded=False
        ):

            col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(
                8, vertical_alignment="top"
            )
            col1.write("**Sector**")
            col2.write("**Historic Fertilizer Demand (Mtpa)**")
            col3.write("**Electrified Share (%)**")
            col4.write("**Remaining Fertilizer Demand (Mtpa)**")
            col5.write("**Domestic Grey Ammonia Supply (Mtpa)**")
            col6.write("**Domestic Grey Ammonia Share (%)**")
            col7.write("**Requested e-Ammonia Share (%)**")
            col8.write("**Required e-Ammonia Production (Mtpa)**")

            old_demand = {}
            new_demand_nh3 = {}
            new_share = {}
            total_demand = 0
            total_remaining_demand = 0
            for s in fertilizeres:
                old_demand[s] = (
                    fertilizeres[s]["demand"] * fertilizeres[s]["ammonia_equiv"]
                )
                col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(
                    8, vertical_alignment="top"
                )
                col1.write(f"**{s}**")
                col2.write(
                    f"{(fertilizeres[s]['demand']*fertilizeres[s]['ammonia_equiv']):.1f} "
                )
                with col3:
                    new_share[s] = st.slider(
                        label=f"Electrification Share {s}",
                        label_visibility="collapsed",
                        min_value=0.0,
                        max_value=100.0,
                        step=1.0,
                        value=fertilizeres[s]["e-share"] * 100,
                        format="%.0f%%",
                        disabled=True,
                    )

                with col4:
                    new_demand_nh3[s] = old_demand[s] * (1 - new_share[s] / 100)
                    st.write(f"{new_demand_nh3[s]:.1f}")

                total_demand += (
                    fertilizeres[s]["demand"] * fertilizeres[s]["ammonia_equiv"]
                )
                total_remaining_demand += new_demand_nh3[s]

            col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(
                8, vertical_alignment="top"
            )
            col1.write("**Total**")
            col2.write(f"{total_demand:.1f}")
            total_electrification_share = (
                total_demand - total_remaining_demand
            ) / total_demand
            col3.slider(
                label=f"Electrification Share {s}",
                label_visibility="collapsed",
                min_value=0.0,
                max_value=100.0,
                step=0.1,
                value=total_electrification_share * 100,
                format="%.1f%%",
                disabled=True,
            )
            col4.write(f"{total_remaining_demand:.1f}")

            with col5:
                domestic_supply = st.slider(
                    label="Domestic Ammonia Supply",
                    label_visibility="collapsed",
                    min_value=0.0,
                    max_value=total_remaining_demand,
                    step=0.1,
                    value=0.4,
                    format="%.1f Mtpa",
                )
            with col6:
                domestic_supply_share = st.slider(
                    label="Domestic Ammonia Share",
                    label_visibility="collapsed",
                    min_value=0.0,
                    max_value=100.0,
                    step=0.1,
                    value=domestic_supply / total_remaining_demand * 100,
                    format="%.1f%%",
                    disabled=True,
                )
            with col7:
                domestic_requested_share = st.slider(
                    label="Requested Ammonia e-Share",
                    label_visibility="collapsed",
                    min_value=0.0,
                    max_value=100.0,
                    step=1.0,
                    value=DEFAULT_E_SHARE_PRODUCTION * 100,
                    format="%.0f%%",
                )
            with col8:
                domestic_requested_demand = st.slider(
                    label="e-Ammonia Demand",
                    label_visibility="collapsed",
                    min_value=0.0,
                    max_value=total_remaining_demand,
                    step=0.1,
                    value=(total_remaining_demand - domestic_supply)
                    * (domestic_requested_share / 100),
                    format="%.1f Mtpa",
                    disabled=True,
                )

            st.write(
                "**Applied NH3 equivalents: Urea=0.57, Ammonia=1.00, MAP=0.15, and DAP=0.26**"
            )
            st.session_state.new_demand_nh3 = domestic_requested_demand

        st.write(
            f"**Considered local e-Ammonia production: {st.session_state.new_demand_nh3:.1f} Mtpa**"
        )

        with st.popover("Project Description", width="stretch", icon="📄"):
            st.write("""
                This application has been developed during a project between **Open Energy Transition** and **Sagax Capital / Keshik Capital** to assess the impact on Australia on local Ammonia and Methanol production.

                The project aims to evaluate the potential for local production of these chemicals using renewable energy sources, and how this does help Australia in its energy transition and resilience.

                **The entire project source is available on GitHub: https://github.com/open-energy-transition/pypsa-aus-efuel.**
                """)


# TAB ECONOMIC PARAMETERS
if t_economic.open:
    with t_economic:
        if st.session_state.n is None:
            st.info("Please load a network via the left sidebar ...")
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

                for d in tech_data:
                    if d == "electrolysis":
                        component = n.links
                        mask = component.carrier.isin(ELECTROLYZER_LINK_CARRIERS)
                    else:
                        component = g
                        mask = component.carrier.str.startswith(d, na=False)

                    if "discount_rate" not in component.columns:
                        component["discount_rate"] = st.session_state.dr / 100

                    old_lt[d] = replace_nan(
                        component.loc[mask, "lifetime"].mean(),
                        tech_data[d]["lt"],
                    )
                    old_dr[d] = (
                        replace_nan(
                            component.loc[mask, "discount_rate"].mean(),
                            tech_data[d]["dr"] / 100,
                        )
                        * 100
                    )
                    old_cc[d] = replace_nan(
                        component.loc[mask, "capital_cost"].mean(),
                        investment_cost(tech_data[d]["cc"], old_dr[d], old_lt[d]),
                    )
                    old_mc[d] = replace_nan(
                        component.loc[mask, "marginal_cost"].mean(),
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
                for d in tech_data:
                    if d == "electrolysis":
                        component = n.links
                        mask = component.carrier.isin(ELECTROLYZER_LINK_CARRIERS)
                    else:
                        component = g
                        mask = component.carrier.str.startswith(d, na=False)

                    if not mask.any():
                        continue

                    if "discount_rate" not in component.columns:
                        component["discount_rate"] = st.session_state.dr / 100

                    component.loc[mask, "discount_rate"] = new_dr[d] / 100
                    component.loc[mask, "capital_cost"] = (
                        new_cc[d]
                        * annuity_factor(new_dr[d] / 100, tech_data[d]["lt"])
                        * (1 + default_om / 100)
                    )
                    component.loc[mask, "marginal_cost"] = new_mc[d]
                    component.loc[mask, "overnight_cost"] = new_cc[d]
                    component.loc[mask, "fom_cost"] = new_cc[d] * default_om / 100

                st.session_state.costs_modified = any(
                    not np.isclose(new_dr[d], old_ui_dr[d])
                    or not np.isclose(new_cc[d], old_ui_cc[d])
                    or not np.isclose(new_mc[d], old_ui_mc[d])
                    for d in tech_data
                )

                st.success("Updated details for mentioned technologies ...")
                st.write(
                    "Remark: in these table the column capital_cost refers to annuity plus fixed O&M costs."
                )

                st.write("Updated generator costs")
                st.dataframe(
                    g[
                        [
                            "carrier",
                            "capital_cost",
                            "marginal_cost",
                            "discount_rate",
                            "overnight_cost",
                            "fom_cost",
                        ]
                    ],
                    height=400,
                )

                st.write("Updated electrolyzer link costs")
                st.dataframe(
                    n.links.loc[
                        n.links.carrier.isin(ELECTROLYZER_LINK_CARRIERS),
                        [
                            "carrier",
                            "capital_cost",
                            "marginal_cost",
                            "discount_rate",
                            "overnight_cost",
                            "fom_cost",
                        ],
                    ],
                    height=400,
                )


# TAB DEMAND PARAMETERS
if t_demand.open:
    with t_demand:
        if st.session_state.n is None:
            st.info("Please load a network via the left sidebar ...")
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

                if not st.session_state.new_demand_meoh is None:
                    old_multiplier["e_methanol"] = st.session_state.new_demand_meoh

                if not st.session_state.new_demand_nh3 is None:
                    old_multiplier["e_ammonia"] = st.session_state.new_demand_nh3

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
                            step=1.0,
                            value=round_multiple(new_cost[l], 0.1),
                            format="%,.0f AUD/t",
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

# TAB OPTIMIZATION
if t_optimization.open:
    with t_optimization:
        if st.session_state.n is None:
            st.info("Please load a network via the left sidebar ...")
            st.write("After loading a network, you are able to optimize the network.")
        else:
            n = st.session_state.n
            new_multiplier = st.session_state.new_multiplier
            new_cost = st.session_state.new_cost

            st.header("Run Optimization")

            network_clusters = infer_network_clusters(n)

            scenario_id = build_scenario_id(clusters=network_clusters)
            scenario_summary = build_scenario_summary(clusters=network_clusters)
            demand = get_current_demand_values()

            ammonia = demand["grey_ammonia"] + demand["e_ammonia"]
            methanol = demand["grey_methanol"] + demand["e_methanol"]

            with st.expander("Scenario Overview", expanded=True):
                st.write(scenario_summary)

            with st.expander("Configuration", expanded=False):
                col1, col2, col3, col4 = st.columns(4)

                col1.metric("Country", "Australia")
                col2.metric("Planning year", "2030")
                col3.metric("Clusters", str(network_clusters))
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

            with st.expander("Solver Options", expanded=False):
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

                    scenario_label = str(len(st.session_state.scenario_labels) + 1)

                    st.session_state.solved_networks[run_name] = n2
                    st.session_state.scenario_metadata[run_name] = scenario_summary
                    st.session_state.scenario_labels[run_name] = scenario_label

                    st.info(f"Saved as scenario {scenario_label}.")

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

# TAB RESULTS
if t_results.open:
    with t_results:
        if len(st.session_state.solved_networks) > 0:
            st.header("Results Explorer")

            if "scenario_metadata" in st.session_state:
                st.subheader("Scenario Overview")

                st.caption(
                    "Hydrogen and hydrogen-based derivative demands are reported in Mtpa."
                )

                scenario_rows = []

                for k, v in st.session_state.scenario_metadata.items():
                    run_nr = st.session_state.scenario_labels.get(k, k)
                    v_split = v.split("|")

                    scenario_rows.append(
                        {
                            "Run": run_nr,
                            "Country": v_split[0].strip(),
                            "Year": v_split[1].strip(),
                            "Clusters": v_split[2].replace("clusters", "").strip(),
                            "Resolution": v_split[3].strip(),
                            "Cost Setup": v_split[4].strip(),
                            "H2 Demand": v_split[5]
                            .replace("Mtpa", "")
                            .replace("H2: ", "")
                            .strip(),
                            "Grey Ammonia": v_split[6]
                            .replace("Mtpa", "")
                            .replace("Grey ammonia: ", "")
                            .strip(),
                            "e-Ammonia": v_split[7]
                            .replace("Mtpa", "")
                            .replace("e-ammonia: ", "")
                            .strip(),
                            "Grey Methanol": v_split[8]
                            .replace("Mtpa", "")
                            .replace("Grey methanol: ", "")
                            .strip(),
                            "e-Methanol": v_split[9]
                            .replace("Mtpa", "")
                            .replace("e-methanol: ", "")
                            .strip(),
                        }
                    )

                scenario_df = pd.DataFrame(scenario_rows)

                st.dataframe(
                    scenario_df,
                    hide_index=True,
                    width="stretch",
                )

            available_runs = list(st.session_state.solved_networks.keys())
            label_map = st.session_state.scenario_labels

            run_lookup = {label_map.get(run, run): run for run in available_runs}

            selected_labels = st.multiselect(
                "Select solved scenarios",
                list(run_lookup.keys()),
                default=list(run_lookup.keys()),
                width="stretch",
            )

            selected_runs = [run_lookup[label] for label in selected_labels]

            st.write("")
            result_view = st.radio(
                "Select result view",
                [
                    "Commodity cost maps",
                    "Installed capacity",
                    "Dispatch",
                    "System costs",
                    # "Technical comparison",
                    "Economic comparison",
                ],
                horizontal=True,
            )

            if selected_runs:
                selected_networks = {
                    label_map.get(run, run): st.session_state.solved_networks[run]
                    for run in selected_runs
                }

                st.subheader(result_view)

                # INSTALLED CAPACITY

                if result_view == "Installed capacity":
                    category = st.radio(
                        "Select result category",
                        get_available_result_categories(),
                        horizontal=True,
                    )

                    if category == "Electricity":
                        cap_df = compute_capacity_by_carrier(
                            selected_networks,
                            category,
                        )
                        y_label = "GW"
                        result_title = "Electricity - Installed capacity"

                    elif category == "CO2 capture":
                        cap_df = compute_annual_flow_by_carrier(
                            selected_networks,
                            category,
                            MWH_PER_TONNE,
                        )
                        y_label = "Mtpa"
                        result_title = "CO2 capture - Annual capture"

                    else:
                        cap_df = compute_annual_flow_by_carrier(
                            selected_networks,
                            category,
                            MWH_PER_TONNE,
                        )
                        y_label = "Mtpa"
                        result_title = f"{category} - Annual production capacity"

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

                        plot_df = chart_df.reset_index().melt(
                            id_vars="scenario",
                            var_name="Technology",
                            value_name="Value",
                        )

                        tech_totals = plot_df.groupby("Technology")["Value"].sum()

                        shown_techs = [
                            tech
                            for tech in DISPATCH_COLORS
                            if tech in tech_totals.index and tech_totals[tech] > 0
                        ]

                        chart = (
                            alt.Chart(plot_df)
                            .mark_bar()
                            .encode(
                                x=alt.X(
                                    "scenario:N",
                                    title="Scenario",
                                    axis=alt.Axis(labelAngle=0),
                                ),
                                y=alt.Y(
                                    "Value:Q",
                                    stack="zero",
                                    title=y_label,
                                ),
                                color=alt.Color(
                                    "Technology:N",
                                    title="Technology",
                                    scale=alt.Scale(
                                        domain=shown_techs,
                                        range=[DISPATCH_COLORS[t] for t in shown_techs],
                                    ),
                                ),
                                tooltip=[
                                    alt.Tooltip("scenario:N"),
                                    alt.Tooltip("Technology:N"),
                                    alt.Tooltip(
                                        "Value:Q",
                                        format=",.2f",
                                    ),
                                ],
                            )
                            .properties(height=600)
                        )

                        st.altair_chart(chart, width="stretch")

                        APP_DIR = Path(__file__).resolve().parent

                        shape_path = (
                            APP_DIR / "data" / "shapes" / "australia_states.geojson"
                        )

                        try:
                            shapes = gpd.read_file(shape_path)

                            map_unit = "GW" if category == "Electricity" else "Mtpa"

                            for capacity_run in selected_runs:
                                capacity_label = label_map.get(
                                    capacity_run,
                                    capacity_run,
                                )

                                st.markdown(f"### Scenario {capacity_label}")

                                capacity_by_bus = compute_capacity_by_bus(
                                    st.session_state.solved_networks[capacity_run],
                                    category,
                                )

                                if capacity_by_bus.empty:
                                    st.warning(
                                        f"No mapped capacity data found for scenario {capacity_label}."
                                    )
                                    continue

                                n_map = st.session_state.solved_networks[capacity_run]

                                map_network = None

                                if category == "Electricity":
                                    map_network = n_map

                                fig = plot_capacity_map_by_bus(
                                    capacity_by_bus,
                                    shapes,
                                    DISPATCH_COLORS,
                                    network=map_network,
                                    unit=map_unit,
                                )

                                st.pyplot(fig, width="content")

                        except Exception as exc:
                            st.error(f"Could not build capacity map: {exc}")

                        table_df = (
                            cap_df.drop(
                                columns=["component"],
                                errors="ignore",
                            )
                            .pivot_table(
                                index=["carrier", "unit"],
                                columns="scenario",
                                values="value",
                                aggfunc="sum",
                                fill_value=0.0,
                            )
                            .reset_index()
                            .rename(
                                columns={
                                    "carrier": "Carrier",
                                    "unit": "Unit",
                                }
                            )
                        )

                        with st.expander(
                            f"Show {category} data table",
                            expanded=False,
                        ):
                            st.dataframe(
                                table_df,
                                width="stretch",
                                hide_index=True,
                            )

                # DISPATCH

                elif result_view == "Dispatch":
                    dispatch_category = st.radio(
                        "Select dispatch category",
                        get_available_dispatch_categories(),
                        horizontal=True,
                    )

                    dispatch_scope = st.radio(
                        "Select dispatch aggregation",
                        ["National", "By state"],
                        horizontal=True,
                    )

                    resample_options = [
                        "Original",
                        "Daily mean",
                        "Weekly mean",
                    ]

                    APP_DIR = Path(__file__).resolve().parent
                    shape_path = (
                        APP_DIR / "data" / "shapes" / "australia_states.geojson"
                    )

                    states = None
                    selected_state = None

                    if dispatch_scope == "By state":
                        try:
                            states = gpd.read_file(shape_path)
                            state_dispatch_all = compute_dispatch_by_carrier_and_state(
                                st.session_state.solved_networks[selected_runs[0]],
                                dispatch_category,
                                states,
                            )

                            available_states = sorted(
                                state_dispatch_all["state"].dropna().unique()
                            )

                            selected_state = st.selectbox(
                                "Select state",
                                available_states,
                            )
                        except Exception as exc:
                            st.error(f"Could not load state shapes: {exc}")
                            st.stop()

                    for dispatch_run in selected_runs:
                        dispatch_label = label_map.get(
                            dispatch_run,
                            dispatch_run,
                        )

                        n_dispatch = st.session_state.solved_networks[dispatch_run]

                        if dispatch_scope == "National":
                            dispatch_df = compute_dispatch_by_carrier(
                                n_dispatch,
                                dispatch_category,
                            )

                        else:
                            state_dispatch = compute_dispatch_by_carrier_and_state(
                                n_dispatch,
                                dispatch_category,
                                states,
                            )

                            if state_dispatch.empty:
                                dispatch_df = pd.DataFrame()

                            else:
                                dispatch_df = (
                                    state_dispatch[
                                        state_dispatch["state"] == selected_state
                                    ]
                                    .drop(columns=["state"])
                                    .set_index("snapshot")
                                )

                                dispatch_df.index = pd.to_datetime(dispatch_df.index)

                        n_snapshots = len(dispatch_df)

                        if n_snapshots <= 100:
                            default_index = 0
                        elif n_snapshots <= 500:
                            default_index = 1
                        else:
                            default_index = 2

                        dispatch_resample = st.selectbox(
                            f"Resample dispatch visualization for scenario {dispatch_label}",
                            resample_options,
                            index=default_index,
                            key=f"dispatch_resample_{dispatch_label}_{dispatch_category}_{dispatch_scope}_{selected_state}",
                        )

                        y_label = "GW" if dispatch_category == "Electricity" else "kt"

                        if dispatch_scope == "National":
                            st.markdown(f"### Scenario {dispatch_label}")
                        else:
                            st.markdown(
                                f"### Scenario {dispatch_label} - {selected_state}"
                            )

                        if dispatch_df.empty:
                            st.warning(
                                f"No dispatch data found for {dispatch_category}"
                                + (
                                    f" in {selected_state}."
                                    if dispatch_scope == "By state"
                                    else "."
                                )
                            )
                            continue

                        plot_dispatch_df = dispatch_df.copy()

                        if dispatch_resample == "Daily mean":
                            plot_dispatch_df = plot_dispatch_df.resample("D").mean()

                        elif dispatch_resample == "Weekly mean":
                            plot_dispatch_df = plot_dispatch_df.resample("W").mean()

                        plot_df = plot_dispatch_df.reset_index().melt(
                            id_vars=(plot_dispatch_df.index.name or "index"),
                            var_name="Technology",
                            value_name="Value",
                        )

                        time_col = plot_dispatch_df.index.name or "index"

                        tech_totals = plot_df.groupby("Technology")["Value"].sum()

                        shown_techs = [
                            tech
                            for tech in DISPATCH_COLORS
                            if tech in tech_totals.index and tech_totals[tech] > 0
                        ]

                        chart = (
                            alt.Chart(plot_df)
                            .mark_area()
                            .encode(
                                x=alt.X(
                                    f"{time_col}:T",
                                    title="Snapshot",
                                ),
                                y=alt.Y(
                                    "Value:Q",
                                    stack="zero",
                                    title=y_label,
                                ),
                                color=alt.Color(
                                    "Technology:N",
                                    title="Technology",
                                    scale=alt.Scale(
                                        domain=shown_techs,
                                        range=[DISPATCH_COLORS[t] for t in shown_techs],
                                    ),
                                ),
                                tooltip=[
                                    alt.Tooltip(
                                        f"{time_col}:T",
                                        title="Snapshot",
                                    ),
                                    alt.Tooltip("Technology:N"),
                                    alt.Tooltip(
                                        "Value:Q",
                                        format=",.2f",
                                    ),
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
                            f"Show {dispatch_category} annual totals for scenario {dispatch_label}",
                            expanded=False,
                        ):
                            st.dataframe(
                                annual_table,
                                width="stretch",
                                hide_index=True,
                            )

                # COMMODITY COST MAPS

                elif result_view == "Commodity cost maps":
                    cost_map = st.radio(
                        "Select cost map",
                        [
                            "Electricity (LCOE)",
                            "H2 from electrolysis (LCOH)",
                            "e-Ammonia levelized cost",
                            "e-Methanol levelized cost",
                        ],
                        horizontal=True,
                    )

                    APP_DIR = Path(__file__).resolve().parent

                    shape_path = (
                        APP_DIR / "data" / "shapes" / "australia_states.geojson"
                    )

                    try:
                        states = gpd.read_file(shape_path)

                        state_maps = []

                        for cost_run in selected_runs:
                            cost_label = label_map.get(cost_run, cost_run)
                            n_cost = st.session_state.solved_networks[cost_run]

                            if cost_map == "Electricity (LCOE)":
                                cost_df, _ = compute_lcoe_by_bus(n_cost)
                                cost_col = "weighted_lcoe"
                                weight_col = "dispatch_twh"
                                output_col = "state_weighted_lcoe"
                                cbar_label = "Generation-weighted LCOE (AUD/MWh)"
                                empty_msg = (
                                    f"No LCOE data found for scenario {cost_label}."
                                )
                                table_title = f"Show state-level LCOE table for scenario {cost_label}"
                                rename_cols = {
                                    "STATE_NAME": "State",
                                    output_col: "Generation-weighted LCOE (AUD/MWh)",
                                    weight_col: "Dispatch (TWh)",
                                }

                            elif cost_map == "H2 from electrolysis (LCOH)":
                                cost_df, _ = compute_lcoh_by_bus(n_cost)
                                cost_col = "weighted_lcoh_aud_per_kg"
                                weight_col = "h2_dispatch_kt"
                                output_col = "state_weighted_lcoh_aud_per_kg"
                                cbar_label = "Production-weighted LCOH (AUD/kg H2)"
                                empty_msg = f"No grid H2 production found for scenario {cost_label}."
                                table_title = f"Show state-level LCOH table for scenario {cost_label}"
                                rename_cols = {
                                    "STATE_NAME": "State",
                                    output_col: "Production-weighted LCOH (AUD/kg H2)",
                                    weight_col: "Grid H2 production (kt H2)",
                                }

                            elif cost_map == "e-Ammonia levelized cost":
                                cost_df, _ = compute_lco_ammonia_by_bus(n_cost)
                                cost_col = "weighted_lco_ammonia_aud_per_tonne"
                                weight_col = "production_kt"
                                output_col = "state_weighted_lco_ammonia_aud_per_tonne"
                                cbar_label = (
                                    "Production-weighted LCO ammonia (AUD/t NH3)"
                                )
                                empty_msg = f"No e-ammonia production found for scenario {cost_label}."
                                table_title = f"Show state-level e-ammonia cost table for scenario {cost_label}"
                                rename_cols = {
                                    "STATE_NAME": "State",
                                    output_col: "Production-weighted LCO ammonia (AUD/t NH3)",
                                    weight_col: "e-ammonia production (kt NH3)",
                                }

                            elif cost_map == "e-Methanol levelized cost":
                                cost_df, _ = compute_lco_methanol_by_bus(n_cost)
                                cost_col = "weighted_lco_methanol_aud_per_tonne"
                                weight_col = "production_kt"
                                output_col = "state_weighted_lco_methanol_aud_per_tonne"
                                cbar_label = "Production-weighted LCOMeOH (AUD/t MeOH)"
                                empty_msg = f"No e-methanol production found for scenario {cost_label}."
                                table_title = f"Show state-level e-methanol cost table for scenario {cost_label}"
                                rename_cols = {
                                    "STATE_NAME": "State",
                                    output_col: "Production-weighted LCOMeOH (AUD/t MeOH)",
                                    weight_col: "e-methanol production (kt MeOH)",
                                }

                            if cost_df.empty:
                                state_maps.append(
                                    (cost_label, None, empty_msg, None, None)
                                )
                                continue

                            state_costs = aggregate_node_costs_by_state(
                                node_df=cost_df,
                                states=states,
                                cost_col=cost_col,
                                weight_col=weight_col,
                                output_cost_col=output_col,
                            )

                            state_maps.append(
                                (
                                    cost_label,
                                    state_costs,
                                    empty_msg,
                                    table_title,
                                    rename_cols,
                                )
                            )

                        valid_state_maps = [
                            state_costs
                            for _, state_costs, _, _, _ in state_maps
                            if state_costs is not None
                            and output_col in state_costs.columns
                            and not state_costs[output_col].dropna().empty
                        ]

                        if not valid_state_maps:
                            st.warning(
                                "No valid cost data available for the selected scenarios."
                            )
                            st.stop()

                        all_values = pd.concat(
                            [
                                state_costs[output_col].dropna()
                                for state_costs in valid_state_maps
                            ],
                            ignore_index=True,
                        )

                        vmin = all_values.quantile(0.05)
                        vmax = all_values.quantile(0.95)

                        for (
                            cost_label,
                            state_costs,
                            empty_msg,
                            table_title,
                            rename_cols,
                        ) in state_maps:
                            st.markdown(f"### Scenario {cost_label}")

                            if state_costs is None:
                                st.warning(empty_msg)
                                continue

                            fig = plot_state_cost_map(
                                state_costs=state_costs,
                                value_col=output_col,
                                colorbar_label=cbar_label,
                                vmin=vmin,
                                vmax=vmax,
                            )

                            st.pyplot(fig, width="content")

                            table_cols = ["STATE_NAME", output_col, weight_col]

                            with st.expander(
                                table_title,
                                expanded=False,
                            ):
                                st.dataframe(
                                    state_costs[table_cols]
                                    .dropna(subset=[output_col])
                                    .round(2)
                                    .rename(columns=rename_cols),
                                    hide_index=True,
                                    width="stretch",
                                )

                    except Exception as exc:
                        st.error(f"Could not build cost map: {exc}")

                # SYSTEM COSTS

                elif result_view == "System costs":
                    system_cost_type = st.radio(
                        "Select system cost type",
                        [
                            "Capital expenditure",
                            "Operational expenditure",
                        ],
                        horizontal=True,
                    )

                    df_system = build_system_cost_table(selected_networks)

                    df_plot = (
                        df_system[df_system["cost_type"] == system_cost_type]
                        .groupby(
                            ["scenario", "tech_label"],
                            as_index=False,
                        )["cost_billion"]
                        .sum()
                    )

                    active_categories = (
                        df_plot.groupby("tech_label")["cost_billion"]
                        .sum()
                        .loc[lambda s: s.abs() > 1e-6]
                        .index
                    )

                    categories = [
                        c for c in renamed_tech_colors if c in active_categories
                    ]

                    df_plot = df_plot[df_plot["tech_label"].isin(categories)]

                    chart = (
                        alt.Chart(df_plot)
                        .mark_bar()
                        .encode(
                            x=alt.X(
                                "scenario:N",
                                title="Scenario",
                                axis=alt.Axis(labelAngle=0),
                            ),
                            y=alt.Y(
                                "cost_billion:Q",
                                title="Annual system cost (Billion AUD/year)",
                                stack="zero",
                            ),
                            color=alt.Color(
                                "tech_label:N",
                                title="Technology",
                                scale=alt.Scale(
                                    domain=categories,
                                    range=[renamed_tech_colors[c] for c in categories],
                                ),
                            ),
                            tooltip=[
                                "scenario",
                                "tech_label",
                                alt.Tooltip(
                                    "cost_billion:Q",
                                    format=",.2f",
                                ),
                            ],
                        )
                        .properties(height=600)
                    )

                    st.altair_chart(chart, width="stretch")

                    summary_table = (
                        df_system[df_system["cost_type"] == system_cost_type]
                        .pivot_table(
                            index=["macro_category", "tech_label"],
                            columns="scenario",
                            values="cost_billion",
                            aggfunc="sum",
                            fill_value=0.0,
                        )
                        .reset_index()
                        .rename(
                            columns={
                                "macro_category": "Macro category",
                                "tech_label": "Technology",
                            }
                        )
                    )

                    scenario_cols = [
                        c
                        for c in summary_table.columns
                        if c not in ["Macro category", "Technology"]
                    ]

                    summary_table = summary_table[
                        (summary_table[scenario_cols].abs().sum(axis=1) > 0)
                    ]

                    with st.expander(
                        f"Show {system_cost_type.lower()} summary table",
                        expanded=False,
                    ):
                        st.dataframe(
                            summary_table.round(3),
                            hide_index=True,
                            width="stretch",
                        )

                    detailed_table = (
                        df_system[df_system["cost_type"] == system_cost_type]
                        .pivot_table(
                            index=[
                                "macro_category",
                                "tech_label",
                                "raw_technology",
                            ],
                            columns="scenario",
                            values="cost_billion",
                            aggfunc="sum",
                            fill_value=0.0,
                        )
                        .reset_index()
                        .rename(
                            columns={
                                "macro_category": "Macro category",
                                "tech_label": "Category",
                                "raw_technology": "Technology",
                            }
                        )
                    )

                    scenario_cols = [
                        c
                        for c in detailed_table.columns
                        if c not in ["Macro category", "Category", "Technology"]
                    ]

                    detailed_table = detailed_table[
                        (detailed_table[scenario_cols].abs().sum(axis=1) > 0)
                    ]

                    with st.expander(
                        f"Show detailed {system_cost_type.lower()} table",
                        expanded=False,
                    ):
                        st.dataframe(
                            detailed_table.round(3),
                            hide_index=True,
                            width="stretch",
                        )

            # ECONOMIC COMPARISON

            if result_view == "Economic comparison":
                df = st.session_state.results
                df = df.rename(columns=label_map)
                df = df / 1e3

                df = df[df.index.get_level_values(0).str.contains("Economics")].round(1)

                df = df.reset_index().drop(columns=["component"]).set_index("carrier")

                st.bar_chart(
                    df.T,
                    x_label="Scenario",
                    y_label="Annual Cost (Million AUD)",
                    horizontal=True,
                )

        else:
            st.info(
                "Please load a network via the left sidebar and run an optimization to see results here ..."
            )

            st.write("""
                After running an optimization, you will see a detailed breakdown of the expanded capacities and economic outcomes for each technology, allowing you to assess the impact of your parameter adjustments on the network's performance and costs.
                """)
