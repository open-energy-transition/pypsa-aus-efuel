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

import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pypsa

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
tech_data: dict[str, dict[str, int | str]] = {
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

load_data: dict[str, dict[str, int | str]] = {
    #    "electricity": {"multiplier": 1, "label": "Electricity"},
    "e-hydrogen": {"multiplier": 1, "label": "eHydrogen", "cost": 2000},
    "e-ammonia": {"multiplier": 1, "label": "eAmmonia", "cost": 700},
    "e-methanol": {"multiplier": 1, "label": "eMethanol", "cost": 700},
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


title = "AUS eFuels"
st.set_page_config(layout="wide", page_title=title)
st.set_page_config(page_title=f"{title} UI", layout="wide")
st.title(f"{title} Interactive Manager")
st.write("Walk through the tabs below from left to the right ...")

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

# --- SIDEBAR ---
with st.sidebar:
    st.sidebar.header("Networks")
    with st.expander("Selected PyPSA Network", expanded=True):
        uploaded_file = st.file_uploader(
            "Choose a PyPSA NetCDF file", type=["nc"], max_upload_size=50  # 50 MB limit
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
            g["discount_rate"] = g.get("discount_rate", st.session_state.dr)
            st.session_state.n = n
            st.session_state.network_loaded = True
            st.success("Network loaded successfully!")

        # Cleanup the temp file
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    if st.sidebar.button("Load Example 'scigrid_de'"):
        n = pypsa.examples.scigrid_de()
        st.session_state.n = n
        st.success("Example network loaded!")

    if st.session_state.network_loaded:
        show_statistics(st.session_state.n)

    st.write("---")
    pkgs = {}
    for pkg in ["highspy", "linopy", "pypsa", "streamlit"]:
        pkg_version = version(pkg)
        pkgs[pkg] = pkg_version

    df = pd.DataFrame.from_dict(pkgs, orient="index", columns=["Version"])
    st.dataframe(df)

# --- Tabs
tabs = [
    "| Welcome",
    "| 1. Economic Parameters",
    "| 2. Demand Parameters ",
    "| 3. Optimization",
    "| 4. View Results",
]
t_welcome, t_economic, t_demand, t_optimization, t_results = st.tabs(
    tabs, on_change="rerun"
)

# --- TAB WeLCOME
if t_welcome.open:
    with t_welcome:
        st.subheader("Welcome to the PyPSA-AUS-eFuels Interactive Manager!")
        st.write(
            """
            This application allows you to load a PyPSA network, adjust key economic parameters, and run optimizations to see how those adjustments impact the network's costs and performance.

            Use the sidebar to load your network and set project targets. Then, navigate through the tabs to manage different aspects of your project.

            This application has been developed during a project between **Open Energy Transition (OET)** and **Sagax Capital / Keshik Capital** to assess the impact on Australia on local Ammonia and Methanol production.

            In 2025, Australia's Diesel consumption was about 32 bn liter or 27.2 MTPA (million ton per annum).
            Ammonia consumption was about 2 MTPA, where about 50% was used in agriculture, 35% in mining and explosives, and the rest in industry and chemicals.

            The table below shows a high-level comparison on population, gross domestic product (GDP), diesel consumption, and agriculture production by State / Territory.

            | State / Territory | Population Share (%) | GDP Share (%) | Diesel Consumption Share (%) | Agriculture Production Share (%) |
            |-----|-----|-----|-----|-----|
            | Western Australia | 11.0% | 17.0% | 24.4% | 21.5% |
            | Queensland | 20.5% | 19.3% | 26.5% | 18.0% |
            | Northern Territory | 1.0% | 1.3% | 3.0% | 1.0% |
            | NSW / ACT | 32.9% | 32.6% | 22.5% | 29.0% |
            | Victoria | 25.6% | 22.7% | 17.4% | 19.0% |
            | South Australia | 6.9% | 5.6% | 4.8% | 9.0% |
            | Tasmania | 2.1% | 1.6% | 1.4% | 2.5% |

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
                """
                After loading a network, you are able to adjust the economic parameters.
                """
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
                new_dr = {}
                old_cc = {}
                new_cc = {}
                old_mc = {}
                new_mc = {}
                for d in tech_data:
                    old_lt[d] = replace_nan(
                        g.loc[g.carrier.str.startswith(d), "lifetime"].mean(),
                        tech_data[d]["lt"],
                    )
                    old_dr[d] = replace_nan(
                        g.loc[g.carrier.str.startswith(d), "discount_rate"].mean(),
                        tech_data[d]["dr"] / 100,
                    )
                    old_cc[d] = replace_nan(
                        g.loc[g.carrier.str.startswith(d), "capital_cost"].mean(),
                        investment_cost(tech_data[d]["cc"], old_dr[d], old_lt[d]),
                    )
                    old_mc[d] = replace_nan(
                        g.loc[g.carrier.str.startswith(d), "marginal_cost"].mean(),
                        tech_data[d]["mc"],
                    )

                col1, col2, col3, col4 = st.columns(4, vertical_alignment="center")
                with col3:
                    st.write("**Capital Cost (AUD/MW)**")
                with col4:
                    st.write("**Marginal Cost (AUD/MWh)**")

                for d in tech_data:
                    col1, col2, col3, col4 = st.columns(4, vertical_alignment="center")
                    with col1:
                        st.write(f"**{tech_data[d]['label']}**")
                    with col2:
                        new_dr[d] = st.slider(
                            label=f"dr_{tech_data[d]['label']}",
                            label_visibility="collapsed",
                            min_value=0.1,
                            max_value=20.0,
                            value=round_multiple(old_dr[d], 0.1),
                            step=0.1,
                            format="%.1f%%",
                        )
                    with col3:
                        new_cc[d] = st.slider(
                            label=f"cc_{tech_data[d]['label']}",
                            label_visibility="collapsed",
                            min_value=1.0,
                            max_value=20_000_000.0,
                            value=investment_cost(old_cc[d], new_dr[d], old_lt[d]),
                            step=0.1,
                            format="%,.1f AUD/MW",
                        )
                    with col4:
                        new_mc[d] = st.slider(
                            label=f"mc_{tech_data[d]['label']}",
                            label_visibility="collapsed",
                            min_value=0.0,
                            max_value=20.0,
                            value=round_multiple(old_mc[d], 0.1),
                            step=0.1,
                            format="%.1f AUD/MWh",
                        )
                st.write(
                    f"Remark: It is assumed to have a fixed O&M with {default_om}%/year for each technology!"
                )

            if st.button("Apply New Costs"):
                g["discount_rate"] = g.get("discount_rate", st.session_state.dr)
                g.loc[g.discount_rate.astype(str).isnull(), "discount_rate"] = (
                    st.session_state.dr
                )

                for d in tech_data:
                    if len(g.carrier[g.carrier == d]):
                        # don't change 'lifetime' for now
                        g.loc[g.carrier.str.startswith(d), "discount_rate"] = new_dr[d]
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

                st.success("Updated details for mentioned technologies ...")
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
                """
                After loading a network, you are able to adjust the demand parameters.
                """
            )
        else:
            st.header("Demand Parameters")
            n = st.session_state.n
            with st.expander("Selected Demand Parameters", expanded=True):
                st.write("Choose Load Multipliers to be used for your case:")
                old_multiplier = {}
                new_multiplier = {}
                new_cost = {}
                # collect the current demand
                for l in load_data:
                    # get the loads associated with the current load, e.g., e-ammonia
                    loads = n.loads[n.loads.carrier.str.contains(l)].index
                    # calculate the sum of the loads collected
                    old_multiplier[l] = (
                        n.loads_t.p[loads].sum().sum() / 1e6
                    )  # convert to MTPA
                #
                col1, col2, col3, col4 = st.columns(4, vertical_alignment="center")
                with col2:
                    st.write("**Old Demand**")
                with col3:
                    st.write("**Proposed Demand**")
                with col4:
                    st.write("**Proposed Price / Tonne**")
                #
                for l in load_data:
                    col1, col2, col3, col4 = st.columns(4, vertical_alignment="center")
                    with col1:
                        st.write(f"**{load_data[l]['label']}**")
                    with col2:
                        st.write(f"{old_multiplier[l]:.1f} MTPA")
                    with col3:
                        new_multiplier[l] = st.slider(
                            label=f"Demand Multiplier {l}",
                            label_visibility="collapsed",
                            min_value=0.0,
                            max_value=20.0,
                            step=0.1,
                            value=round_multiple(old_multiplier[l], 0.1),
                            format="%.1f MTPA",
                        )
                    with col4:
                        new_cost[l] = st.slider(
                            label=f"Cost {l}",
                            label_visibility="collapsed",
                            min_value=0.0,
                            max_value=5000.0,
                            step=0.1,
                            value=round_multiple(load_data[l]["cost"], 0.1),
                            format="%.1f AUD/Tonne",
                        )
                st.session_state.old_multiplier = old_multiplier
                st.session_state.new_multiplier = new_multiplier
                st.session_state.new_cost = new_cost

            if st.button("Apply New Demand"):
                name_loads = []
                for l in load_data:
                    nr_loads = len(n.loads.index[n.loads.carrier.str.contains(l)])
                    for load in n.loads.index[n.loads.carrier.str.contains(l)]:
                        # update the recent load with the adjusted load
                        if old_multiplier[l] > 0:
                            n.loads.loc[load, "p_set"] = (
                                new_multiplier[l]
                                * 1e6
                                / 8760
                                / old_multiplier[l]
                                / nr_loads
                            )
                        else:
                            n.loads.loc[load, "p_set"] = (
                                new_multiplier[l] * 1e6 / 8760 / nr_loads
                            )
                        n.loads_t.p[load] = n.loads.loc[load, "p_set"]
                        # save the name of the load for later use
                        name_loads.append(load)

                st.success("Updated details for mentioned carriers ...")
                df = n.loads[["carrier", "p_set"]]
                st.dataframe(
                    df[
                        df[["carrier", "p_set"]]
                        .index.str.lower()
                        .isin([n.lower() for n in name_loads])
                    ]
                )

# --- TAB OPTIMIZATION
if t_optimization.open:
    with t_optimization:
        if st.session_state.n is None:
            st.info("Please load a network ...")
            st.write(
                """
                After loading a network, you are able to optimize the network.
                """
            )
        else:
            n = st.session_state.n
            old_multiplier = st.session_state.old_multiplier
            new_multiplier = st.session_state.new_multiplier
            st.header("Run Optimization")
            with st.expander("Snapshot Options", expanded=True):
                col1, col2, col3 = st.columns(3, vertical_alignment="top")
                with col1:
                    run_mode = st.radio(
                        "Select the number of desired optimization snapshots:",
                        [
                            "Full Year",
                            "Week per Month",
                        ],
                        index=1,
                        horizontal=True,
                    )

                with col2:
                    weeks = st.radio(
                        "In case of 'Week per Month', select the week within the month to consider ...",
                        [1, 2, 3, 4],
                        index=1,
                        horizontal=True,
                    )
                    months = st.multiselect(
                        "... and the months to consider!",
                        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                        default=[1, 4, 7, 10],
                    )

            with st.expander("Solver Options", expanded=True):
                solver_name = st.radio(
                    "Select the solver to use for optimization:",
                    [
                        "highs",
                        "OETC",
                    ],
                    index=0,
                    horizontal=True,
                )

            if st.button("Run LOPF"):
                n2 = n.copy()
                g = n2.generators

                if run_mode == "Week per Month":
                    start_day = (weeks - 1) * 7 + 1
                    end_day = start_day + 6
                    months = months  # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
                else:
                    start_day = 1
                    end_day = start_day + 1
                    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

                if run_mode != "Full Year":
                    sns_before = len(n2.snapshots)
                    sns_subset = get_snapshots(
                        n2, start_day=start_day, end_day=end_day, months=months
                    )
                    sns_after = len(sns_subset)
                    n2.set_snapshots(sns_subset)
                    n2.snapshot_weightings = (
                        n2.snapshot_weightings * sns_before / sns_after
                    )

                st.info(f"Optimizing for {len(n2.snapshots)} snapshots ...")
                st.session_state.opt_runs += 1
                with st.spinner("Solving Network ..."):
                    n2.consistency_check()
                    status, condition = n2.optimize(
                        solver_name=solver_name,
                        assign_all_duals=True,
                        include_objective_constant=False,
                        solver_options={
                            "user_objective_scale": -2,
                            "user_bound_scale": -14,
                        },
                    )

                if status == "ok":
                    # Show Results
                    st.success(f"Optimization finished: {condition}")
                    # st.metric("Total System Cost", f"${n2.objective:,.2f}")
                    st.subheader("Results: Generation Dispatch")
                    dispatch = n2.generators_t.p  # .sum()
                    st.bar_chart(dispatch, y_label="MW")
                    # calculate the annual costs for importing e-fuels otherwise
                    new_cost = st.session_state.new_cost
                    avoided_import_cost = 0
                    for l in load_data:
                        avoided_import_cost += new_multiplier[l] * new_cost[l] * 1e6
                    #
                    old_total_cost = avoided_import_cost
                    new_total_cost = n2.objective
                    expanded_cap = n2.statistics.expanded_capacity().round(1)
                    #
                    # expanded_cap[('Economics', 'Old Annuity')] = round(old_total_cost / 1e6, 1) # convert to million AUD
                    expanded_cap[("Economics", "New Annuity")] = round(
                        n2.objective / 1e6, 1
                    )  # convert to million AUD
                    expanded_cap[("Economics", "Savings")] = round(
                        (old_total_cost - new_total_cost) / 1e6, 1
                    )  # convert to million AUD
                    #
                    if st.session_state.results is None:
                        cap_df = expanded_cap.to_frame(
                            name=f"run {st.session_state.opt_runs}"
                        )
                    else:
                        cap_df = st.session_state.results.join(
                            expanded_cap.to_frame(
                                name=f"run {st.session_state.opt_runs}"
                            )
                        )
                    #
                    # save the cap_df to be used in the 'View Results' tab
                    st.session_state.results = cap_df
                    st.write(
                        """
                        Successfully optimized the network with the new parameters.

                        Check the 'View Results' tab for details.
                        """
                    )
                else:
                    st.error(f"Solver failed: {condition}")

# ---TAB RESULTS
if t_results.open:
    with t_results:
        if st.session_state.results is not None:
            st.header("Technical Comparison")
            st.write(
                "Only the technologies being different are shown in the table below, while the economic comparison is shown in the chart below."
            )
            df = st.session_state.results
            # don't show economic details in the technical comparison
            df = df[~df.index.get_level_values(0).str.contains("Economics")]
            # only show rows where there is a difference in the values across runs
            df = df[df.nunique(axis=1) > 1]
            st.dataframe(df.T.style.format("{:.1f}"))
            st.header("Economic Comparison")
            #
            df = st.session_state.results
            df = df / 1e3  # convert to million AUD
            # only show economic details
            df = df[df.index.get_level_values(0).str.contains("Economics")].round(1)
            df = df.reset_index().drop(columns=["component"])
            df = df.set_index("carrier")
            st.bar_chart(df.T, y_label="Runs", x_label="Million AUD", horizontal=True)
        else:
            st.info(
                "Please load a network and run an optimization to see results here ..."
            )
            st.write(
                """
                After running an optimization, you will see a detailed breakdown of the expanded capacities and economic outcomes for each technology, allowing you to assess the impact of your parameter adjustments on the network's performance and costs.
                """
            )
