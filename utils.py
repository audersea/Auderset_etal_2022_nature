"""
Utilities for performing analyses and useful values for limiting or filtering
data.
"""
from loguru import logger
from pathlib import Path
from typing import Tuple, List, Callable, Optional
import pandas as pd

INPUT_DATA_PATH = Path("data", "input")
GRADIENT_PATH = Path("data", "gradients")

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


def get_SST_source_data():
    """Get the main source data (to be updated with Pangaea)"""
    return pd.read_excel(
        INPUT_DATA_PATH / "Table S2_SST compilation.xlsx", sheet_name=0, skiprows=[1]
    )


def get_SST_gradient_data():
    """Get the gradient SST high latitudes vs mid and low latitudes"""
    input_path = GRADIENT_PATH / "SST_LOESS_MI0pt4_RS30_BIT0pt4_RT0pt7_gradients.csv"
    if not input_path.exists():
        raise FileNotFoundError(
            "SST gradient data not found, try running the SST_gradient notebook"
        )
    return pd.read_csv(input_path, index_col=0)


def get_d15N_source_data():
    """Get the source d15N Atlantic and Pacific data"""
    input_path = INPUT_DATA_PATH / "d15N_A_P.csv"
    return pd.read_csv(input_path)


def get_d15N_gradient_data():
    """Get the gradient d15N Atlantic and Pacific data"""
    input_path = GRADIENT_PATH / "d15N_A_P_gradient.csv"
    if not input_path.exists():
        raise FileNotFoundError(
            "d15N gradient data not found, try running the d15N_gradient notebook"
        )
    return pd.read_csv(input_path, index_col=0)


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
