# SPDX-FileCopyrightText: Open Energy Transition gGmbH
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


NH3_MWH_PER_TON = 5.17
MEOH_MWH_PER_TON = 5.54


VALID_DEMAND_ALLOCATION_MODES = {
    "proportional_existing_capacity",
    "brownfield_optimised_growth",
    "greenfield_optimised_growth",
}


def get_demand_allocation_mode(config: dict) -> str:
    """
    Read custom industry demand allocation mode from config.
    """
    mode = (
        config.get("custom_industry", {})
        .get("demand_allocation", {})
        .get("mode", "proportional_existing_capacity")
    )

    if mode not in VALID_DEMAND_ALLOCATION_MODES:
        raise ValueError(
            f"Invalid custom industry demand allocation mode '{mode}'. "
            f"Expected one of {sorted(VALID_DEMAND_ALLOCATION_MODES)}."
        )

    return mode


def load_gem_data(path: str) -> pd.DataFrame:
    """
    Load GEM plant-level data and keep only Australian ammonia/methanol plants.
    """
    df = pd.read_excel(path, sheet_name="Plant data")

    df["Primary products"] = df["Primary products"].str.strip().str.lower()
    df["Country/area"] = df["Country/area"].str.strip()

    df = df[
        (df["Country/area"] == "Australia")
        & (df["Primary products"].isin(["ammonia", "methanol"]))
    ].copy()

    coords = df["Coordinates"].str.split(",", expand=True)
    df["y"] = pd.to_numeric(coords[0], errors="coerce")
    df["x"] = pd.to_numeric(coords[1], errors="coerce")

    logger.info(f"Loaded {len(df)} Australian ammonia/methanol plants from GEM.")

    return df


def load_capacity_data(path: str) -> pd.DataFrame:
    """
    Load manually curated plant-level production capacities.
    """
    df = pd.read_excel(path)

    required_cols = [
        "GEM plant ID",
        "Production capacity (tpa)",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in capacity file: {missing}")

    df["Production capacity (tpa)"] = pd.to_numeric(
        df["Production capacity (tpa)"], errors="coerce"
    )

    logger.info(f"Loaded {len(df)} plant-level capacity records.")

    return df


def merge_data(gem_df: pd.DataFrame, cap_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge GEM location data with manually curated capacity data using GEM plant ID.
    """
    keep_cols = ["GEM plant ID", "Production capacity (tpa)"]
    if "Source" in cap_df.columns:
        keep_cols.append("Source")

    df = gem_df.merge(
        cap_df[keep_cols],
        on="GEM plant ID",
        how="left",
        validate="one_to_one",
    )

    missing = df["Production capacity (tpa)"].isna().sum()
    if missing > 0:
        logger.warning(f"{missing} plants are missing production capacity after merge.")

    return df


def allocate_and_split(
    df: pd.DataFrame,
    targets: dict,
    e_shares: dict,
    mode: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Allocate custom industry demand.

    The CSV production capacity is always treated as the existing baseline.
    The baseline is always grey.

    The e_share is applied only to the demand growth, not to the baseline.

    Modes:
    - proportional_existing_capacity:
        Growth is allocated proportionally to existing plant capacity.
    - brownfield_optimised_growth:
        Growth is not allocated here. It is returned as an aggregate target
        to be optimised later on existing industry nodes.
    - greenfield_optimised_growth:
        Growth is not allocated here. It is returned as an aggregate target
        to be optimised later on candidate nodes.
    """
    df = df.copy()
    df["industry"] = df["Primary products"]

    growth_records = []

    for product, target in targets.items():
        mask = df["industry"] == product

        baseline = df.loc[mask, "Production capacity (tpa)"].fillna(0.0)
        baseline_total = baseline.sum()

        if baseline_total == 0:
            raise ValueError(f"No baseline capacity found for product '{product}'.")

        growth = target - baseline_total

        if growth < 0:
            raise ValueError(
                f"Target for '{product}' ({target:.2f} tpa) is lower than "
                f"baseline CSV capacity ({baseline_total:.2f} tpa). "
                "Downscaling is not implemented for custom industry demand."
            )

        e_share = e_shares.get(product, 0.0)

        if not 0 <= e_share <= 1:
            raise ValueError(f"e_share for '{product}' must be between 0 and 1.")

        df.loc[mask, f"e_{product}_tpa"] = 0.0
        df.loc[mask, f"grey_{product}_tpa"] = baseline

        if mode == "proportional_existing_capacity":
            shares = baseline / baseline_total

            allocated_e_growth = shares * growth * e_share
            allocated_grey_growth = shares * growth * (1 - e_share)

            df.loc[mask, f"e_{product}_tpa"] += allocated_e_growth
            df.loc[mask, f"grey_{product}_tpa"] += allocated_grey_growth

            growth_for_model = 0.0
        else:
            growth_for_model = growth

        growth_records.extend(
            [
                {
                    "product": product,
                    "carrier": f"e_{product}",
                    "growth_tpa": growth_for_model * e_share,
                },
                {
                    "product": product,
                    "carrier": f"grey_{product}",
                    "growth_tpa": growth_for_model * (1 - e_share),
                },
            ]
        )

        logger.info(
            f"{product}: baseline = {baseline_total:.2f} tpa, "
            f"target = {target:.2f} tpa, growth = {growth:.2f} tpa, "
            f"e_share_on_growth = {e_share:.2f}, mode = {mode}"
        )

    growth_targets = pd.DataFrame(growth_records)

    return df, growth_targets


def convert_to_mwh(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert allocated production from tpa to MWh/a.
    """
    df = df.copy()

    df["e_ammonia"] = df.get("e_ammonia_tpa", 0.0) * NH3_MWH_PER_TON
    df["grey_ammonia"] = df.get("grey_ammonia_tpa", 0.0) * NH3_MWH_PER_TON

    df["e_methanol"] = df.get("e_methanol_tpa", 0.0) * MEOH_MWH_PER_TON
    df["grey_methanol"] = df.get("grey_methanol_tpa", 0.0) * MEOH_MWH_PER_TON

    return df


def convert_growth_targets_to_mwh(growth_targets: pd.DataFrame) -> pd.DataFrame:
    """
    Convert aggregate growth targets from tpa to MWh/a.
    """
    growth_targets = growth_targets.copy()

    factors = {
        "ammonia": NH3_MWH_PER_TON,
        "methanol": MEOH_MWH_PER_TON,
    }

    growth_targets["conversion_factor_mwh_per_t"] = growth_targets["product"].map(
        factors
    )

    if growth_targets["conversion_factor_mwh_per_t"].isna().any():
        missing = growth_targets.loc[
            growth_targets["conversion_factor_mwh_per_t"].isna(), "product"
        ].unique()
        raise ValueError(f"Missing conversion factors for products: {missing}")

    growth_targets["growth_mwh"] = (
        growth_targets["growth_tpa"] * growth_targets["conversion_factor_mwh_per_t"]
    )

    return growth_targets[
        [
            "product",
            "carrier",
            "growth_tpa",
            "growth_mwh",
            "conversion_factor_mwh_per_t",
        ]
    ]


def prepare_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare plant-level dataset for spatial mapping.
    """
    df = df.copy()
    df["country"] = "AU"

    before = len(df)
    df = df.dropna(subset=["x", "y"])
    dropped = before - len(df)

    if dropped > 0:
        logger.warning(f"Dropped {dropped} plants due to missing coordinates.")

    return df


def explode_by_carrier(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expand each plant into one record per custom industry carrier.
    """
    records = []

    carriers = [
        "grey_ammonia",
        "e_ammonia",
        "grey_methanol",
        "e_methanol",
    ]

    for _, row in df.iterrows():
        for carrier in carriers:
            value = row.get(carrier, 0.0)

            if pd.notna(value) and value > 0:
                records.append(
                    {
                        "country": row["country"],
                        "x": row["x"],
                        "y": row["y"],
                        "capacity": value,
                        "industry": carrier,
                    }
                )

    out = pd.DataFrame(records)

    if out.empty:
        raise ValueError("No custom industry records were created after expansion.")

    return out


def map_industry_to_buses(df: pd.DataFrame, shapes_path: str) -> pd.DataFrame:
    """
    Map plant-level industry records to clustered onshore bus regions.
    """
    gdf = gpd.GeoDataFrame(
        df.copy(),
        geometry=gpd.points_from_xy(df["x"], df["y"]),
        crs="EPSG:4326",
    )

    regions = gpd.read_file(shapes_path)

    if regions.crs is None:
        regions = regions.set_crs("EPSG:4326")
    else:
        regions = regions.to_crs("EPSG:4326")

    mapped = gpd.sjoin(
        gdf,
        regions,
        how="left",
        predicate="within",
    )

    bus_col_candidates = ["name", "Name", "bus", "Bus", "gadm_1"]
    bus_col = next((c for c in bus_col_candidates if c in mapped.columns), None)

    if bus_col is None:
        raise ValueError(
            "Could not identify bus column after spatial join. "
            f"Available columns: {list(mapped.columns)}"
        )

    mapped = mapped.rename(columns={bus_col: "bus"})

    missing = mapped["bus"].isna().sum()
    if missing > 0:
        raise ValueError(
            f"{missing} custom industry plants could not be mapped to bus regions."
        )

    if "country" not in mapped.columns:
        if "country_left" in mapped.columns:
            mapped = mapped.rename(columns={"country_left": "country"})
        else:
            mapped["country"] = "AU"

    return pd.DataFrame(
        mapped.drop(columns=["geometry", "index_right"], errors="ignore")
    )


def aggregate_by_bus(mapped_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate mapped plant capacities by bus and custom industry carrier.
    """
    expected_columns = [
        "grey_ammonia",
        "e_ammonia",
        "grey_methanol",
        "e_methanol",
    ]

    industrial_demand = (
        mapped_df.groupby(["bus", "industry", "country"])["capacity"]
        .sum()
        .reset_index()
    )

    industrial_demand = (
        industrial_demand.pivot(
            index=["bus", "country"],
            columns="industry",
            values="capacity",
        )
        .reset_index()
        .fillna(0)
    )

    for col in expected_columns:
        if col not in industrial_demand.columns:
            industrial_demand[col] = 0.0

    return industrial_demand[["bus", "country"] + expected_columns]


if __name__ == "__main__":
    config = snakemake.config

    shapes_path = snakemake.input.shapes_path
    gem_path = snakemake.input.gem_data
    capacity_path = snakemake.input.capacity_data
    targets = config["custom_industry"]["targets_tpa"]
    e_shares = config["custom_industry"]["e_share"]

    gem_df = load_gem_data(gem_path)
    cap_df = load_capacity_data(capacity_path)

    df = merge_data(gem_df, cap_df)

    mode = get_demand_allocation_mode(config)

    df, growth_targets = allocate_and_split(
        df,
        targets,
        e_shares,
        mode,
    )

    df = convert_to_mwh(df)
    growth_targets = convert_growth_targets_to_mwh(growth_targets)

    df = prepare_mapping(df)

    if hasattr(snakemake.output, "plants"):
        df.to_csv(snakemake.output.plants, index=False)
        logger.info("Saved merged plant-level custom industry dataset.")

    if hasattr(snakemake.output, "growth_targets"):
        logger.warning(
            f"Writing growth_targets to {snakemake.output.growth_targets} | "
            f"mode={mode} | targets={targets} | e_share={e_shares}"
        )
        growth_targets.to_csv(snakemake.output.growth_targets, index=False)

    df_expanded = explode_by_carrier(df)

    mapped_df = map_industry_to_buses(df_expanded, shapes_path)

    industrial_demand = aggregate_by_bus(mapped_df)

    industrial_demand.to_csv(
        snakemake.output.industrial_energy_demand_per_node,
        index=False,
    )

    logger.info("Custom AU industry demand built successfully.")
