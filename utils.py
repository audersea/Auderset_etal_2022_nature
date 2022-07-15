"""
Utilities for performing analyses and useful values for limiting or filtering
data.
"""
from loguru import logger
from pathlib import Path
from typing import Tuple, List, Callable, Optional
import pandas as pd

INPUT_DATA_PATH = Path("data", "input")

# Methane Index
MI_FILLNA = -999
MI_THRESHOLD = 0.4
# Red sea type GDGTs
GDGTRS_FILLNA = -999
GDGTRS_THRESHOLD = 30
# Crenarchaeol areas
CREN_FILLNA = 9999
CREN_THRESHOLD = 1000
# Branched and Isoprenoid Index
BIT_FILLNA = -999
BIT_THRESHOLD = 0.4
# Cyclisation degree of tetramethylated brGDGTs
RINGSTETRA_FILLNA = 999
RINGSTETRA_THRESHOLD = 0.7


def get_source_data():
    """Get the main source data (to be updated with Pangaea)"""
    return pd.read_excel(
        INPUT_DATA_PATH / "Table S2_SST compilation.xlsx", sheet_name=0, skiprows=[1]
    )


def fill_nans(df: pd.DataFrame) -> pd.DataFrame:
    """Fill any missing values with some defaults"""
    df["MI"] = df["MI"].fillna(MI_FILLNA)
    df["%GDGTrs"] = df["%GDGTrs"].fillna(GDGTRS_FILLNA)
    df["Cren'"] = df["Cren'"].fillna(CREN_FILLNA)
    df["BIT"] = df["BIT"].fillna(BIT_FILLNA)
    df["#ringstetra"] = df["#ringstetra"].fillna(RINGSTETRA_FILLNA)
    return df


def numf(x: float) -> str:
    """Format a number for a filename"""
    return str(x).replace(".", "pt")


def filter_mi(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """Filter on methane index"""
    logger.info(f"Filtering on methane index < {MI_THRESHOLD}")
    logger.info(f"number of rows pre filter = {len(df.index)}")
    filtered = df[df["MI"] < MI_THRESHOLD].copy()
    logger.info(f"number of rows post filter = {len(filtered.index)}")
    return filtered, f"MI{numf(MI_THRESHOLD)}"


def filter_gdgtrs(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """Filter on GDGTRS"""
    logger.info(f"Filtering on GDGTRS < {GDGTRS_THRESHOLD}")
    logger.info(f"number of rows pre filter = {len(df.index)}")
    filtered = df[df["%GDGTrs"] < GDGTRS_THRESHOLD].copy()
    logger.info(f"number of rows post filter = {len(filtered.index)}")
    return filtered, f"RS{numf(GDGTRS_THRESHOLD)}"


def filter_cren(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """Filter on CREN"""
    logger.info(f"Filtering on CREN > {CREN_THRESHOLD}")
    logger.info(f"number of rows pre filter = {len(df.index)}")
    filtered = df[df["Cren'"] > CREN_THRESHOLD].copy()
    logger.info(f"number of rows post filter = {len(filtered.index)}")
    return filtered, f"CR{numf(CREN_THRESHOLD)}"


def filter_bit_ringstetra(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """Filter using a combination of BIT and ringstetra"""
    logger.info(
        f"Filtering on BIT > {BIT_THRESHOLD} and RINGSTETRA < {RINGSTETRA_THRESHOLD}"
    )
    logger.info(f"number of rows pre filter = {len(df.index)}")
    filtered = df[
        ~((df["BIT"] > BIT_THRESHOLD) & (df["#ringstetra"] < RINGSTETRA_THRESHOLD))
    ].copy()
    logger.info(f"number of rows post filter = {len(filtered.index)}")
    postpend = f"BIT{numf(BIT_THRESHOLD)}_RT{numf(RINGSTETRA_THRESHOLD)}"
    return filtered, postpend


def apply_filters(
    df: pd.DataFrame, filters: List[Callable]
) -> Tuple[pd.DataFrame, str]:
    """Apply multiple filters in one go"""
    filtered = df
    postpend = ""
    for filt in filters:
        filtered, poststr = filt(filtered)
        postpend += f"_{poststr}"
    return filtered, postpend


def apply_final_filtering(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """Apply the final filtering selection"""
    filters = [filter_mi, filter_gdgtrs, filter_bit_ringstetra]
    return apply_filters(df, filters)


def filter_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"number of rows = {len(df.index)}")

    filtered = df[
        (df["MI"] < MI_THRESHOLD)
        & (df["%GDGTrs"] < GDGTRS_THRESHOLD)
        & (df["Cren'"] > CREN_THRESHOLD)
    ].copy()
    logger.info(f"number of rows filter MI, GDGTRS, Cren = {len(filtered.index)}")

    # filtered = filtered[
    #     ~(
    #         (filtered["BIT"] > BIT_THRESHOLD)
    #         & (filtered["#ringstetra"] < RINGSTETRA_THRESHOLD)
    #     )
    # ].copy()
    # logger.info(f"number of rows filter BIT and ringstetra = {len(filtered.index)}")

    return filtered
