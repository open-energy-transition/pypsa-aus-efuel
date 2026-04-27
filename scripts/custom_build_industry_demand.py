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


def allocate_and_split(df: pd.DataFrame, targets: dict, e_shares: dict) -> pd.DataFrame:
    """
    Allocate 2030 total production targets proportionally to historical capacity,
    then split them into grey and e- fractions according to config shares.
    """
    df = df.copy()
    df["industry"] = df["Primary products"]
    df["total_2030_tpa"] = 0.0

    for product, target in targets.items():
        mask = df["industry"] == product
        total_capacity = df.loc[mask, "Production capacity (tpa)"].sum()

        if total_capacity == 0:
            raise ValueError(f"No production capacity found for product '{product}'.")

        shares = df.loc[mask, "Production capacity (tpa)"] / total_capacity
        df.loc[mask, "total_2030_tpa"] = shares * target

        logger.info(
            f"{product}: historical total = {total_capacity:.2f} tpa, "
            f"2030 target = {target:.2f} tpa"
        )

    for product in targets.keys():
        e_share = e_shares.get(product, 0.0)

        if not 0 <= e_share <= 1:
            raise ValueError(f"e_share for '{product}' must be between 0 and 1.")

        mask = df["industry"] == product

        df.loc[mask, f"e_{product}_tpa"] = df.loc[mask, "total_2030_tpa"] * e_share
        df.loc[mask, f"grey_{product}_tpa"] = df.loc[mask, "total_2030_tpa"] * (
            1 - e_share
        )

    return df


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
    df = allocate_and_split(df, targets, e_shares)
    df = convert_to_mwh(df)
    df = prepare_mapping(df)

    if hasattr(snakemake.output, "plants"):
        df.to_csv(snakemake.output.plants, index=False)
        logger.info("Saved merged plant-level custom industry dataset.")

    df_expanded = explode_by_carrier(df)

    mapped_df = map_industry_to_buses(df_expanded, shapes_path)

    industrial_demand = aggregate_by_bus(mapped_df)

    industrial_demand.to_csv(
        snakemake.output.industrial_energy_demand_per_node,
        index=False,
    )

    logger.info("Custom AU industry demand built successfully.")
