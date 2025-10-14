import pandas as pd
import numpy as np
from typing import Tuple


def extract_remaining_lease_years(lease_str: str) -> float:
    """
    Extracts remaining lease as a number in years from string format that includes months.
    
    Args:
        lease_str: String
        
    Returns:
        float: Years remaining
    """
    if pd.isna(lease_str):
        return np.nan
    
    parts = str(lease_str).split()
    
    years = 0.0
    months = 0.0
    
    for i, part in enumerate(parts):
        lowered_part = part.lower()
        if lowered_part == 'years' and i > 0:
            years = float(parts[i-1])
        elif lowered_part == 'months' and i > 0:
            months = float(parts[i-1])
    
    total_years = years + (months / 12.0)
    return round(total_years, 2)


def validate_storey_range_format(storey_range: str) -> bool:
    """
    Validates that storey range follows the expected format of ''XX TO YY'.
    """
    if pd.isna(storey_range):
        return False
    
    try:
        parts = str(storey_range).strip().split(' TO ')
        
        if len(parts) != 2:
            return False
        
        lower = parts[0].strip()
        upper = parts[1].strip()
        
        if not (lower.isdigit() and upper.isdigit()):
            return False
        
        if not (len(lower) == 2 and len(upper) == 2):
            return False
        
        if int(upper) <= int(lower):
            return False
        
        return True
        
    except (ValueError, AttributeError):
        return False

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and preprocesses the HDB data by checking for missing values, converting data types
    and extracting useful features.
    """    
    clean_df = df.copy()
    
    # Checks for missing values and remove rows with critical missing values
    missing_counter = clean_df.isnull().sum()
    if missing_counter.sum() > 0:
        print("Missing values found:", missing_counter[missing_counter > 0])
    else:
        print("No missing values found")
    
    initial_rows = len(clean_df)
    clean_df = clean_df.dropna(subset=['resale_price', 'floor_area_sqm', 'town', 'flat_type'])
    rows_removed = initial_rows - len(clean_df)

    if rows_removed > 0:
        print(f"Removed {rows_removed} rows with missing critical values")
    else:
        print("No rows removed")
    
    invalid_storey = ~clean_df['storey_range'].apply(validate_storey_range_format)
    invalid_count = invalid_storey.sum()
    
    if invalid_count > 0:
        print(f"Warning: Found {invalid_count} records with invalid storey_range format")
        print("Sample invalid values:")
        print(clean_df[invalid_storey]['storey_range'].value_counts().head())
        clean_df = clean_df[~invalid_storey]
    else:
        print("All storey_range values are valid")

    # Ensures string columns are strings and uppercased for case-insensitive filtering 
    clean_df['town'] = clean_df['town'].astype(str).str.upper()
    clean_df['flat_type'] = clean_df['flat_type'].astype(str).str.upper()
    clean_df['flat_model'] = clean_df['flat_model'].astype(str).str.upper()
    clean_df['storey_range'] = clean_df['storey_range'].astype(str).str.upper()

    # Ensures numeric columns are numeric
    clean_df['floor_area_sqm'] = pd.to_numeric(clean_df['floor_area_sqm'], errors='coerce')
    clean_df['lease_commence_date'] = pd.to_numeric(clean_df['lease_commence_date'], errors='coerce')
    clean_df['resale_price'] = pd.to_numeric(clean_df['resale_price'], errors='coerce')
    clean_df = clean_df.rename(columns={'month': 'transaction_date'})

    clean_df['remaining_lease_years'] = clean_df['remaining_lease'].apply(
        extract_remaining_lease_years
    )
    
    print("Features extracted")
    
    return clean_df


def get_processing_summary(df: pd.DataFrame) -> dict:
    df_temp = df.copy()
    df_temp['transaction_datetime'] = pd.to_datetime(df_temp["transaction_date"], format='%Y-%m', errors='coerce')
    summary = {
        'total_records': len(df),
        'date_range': {
            'earliest': df_temp['transaction_datetime'].min().strftime('%Y-%m'),
            'latest': df_temp['transaction_datetime'].max().strftime('%Y-%m')
        },
        'price_stats': {
            'min': df['resale_price'].min(),
            'max': df['resale_price'].max(),
            'median': df['resale_price'].median(),
            'mean': df['resale_price'].mean()
        },
        'unique_towns': df['town'].nunique(),
        'unique_flat_types': df['flat_type'].nunique(),
        'towns': sorted(df['town'].unique().tolist()),
        'flat_types': sorted(df['flat_type'].unique().tolist())
    }
    return summary


def preprocess_hdb_data(filepath: str, verbose: bool = True) -> Tuple[pd.DataFrame, dict]:
    df = pd.read_csv(filepath)
    print(f"Loaded {len(df)} rows and {len(df.columns)} columns")

    clean_df = clean_data(df)
    
    summary = get_processing_summary(clean_df)

    if verbose:
        print(f"\nDataset Summary:")
        print(f"  Total records: {summary['total_records']:,}")
        print(f"  Date range: {summary['date_range']['earliest']} to {summary['date_range']['latest']}")
        print(f"  Price range: ${summary['price_stats']['min']:,.0f} - ${summary['price_stats']['max']:,.0f}")
        print(f"  Median price: ${summary['price_stats']['median']:,.0f}")
        print(f"  Number of towns: {summary['unique_towns']}")
        print(f"  Number of flat types: {summary['unique_flat_types']}")
    
    return clean_df, summary


if __name__ == "__main__":
    df, summary = preprocess_hdb_data('../ResaleFlatPricesData.csv')
    
    print("\nNumber of columns:", len(df.columns))
    print("\nColumn names:")
    print(df.columns.tolist())
    
    print("\nData types:")
    print(df.dtypes)
