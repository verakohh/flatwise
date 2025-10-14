import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import time

def create_price_mask(df: pd.DataFrame, 
                     min_price: Optional[float] = None,
                     max_price: Optional[float] = None) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    if min_price is not None:
        mask &= (df['resale_price'] >= min_price)
    if max_price is not None:
        mask &= (df['resale_price'] <= max_price)
    return mask


def create_towns_mask(df: pd.DataFrame, 
                     towns: Optional[List[str]] = None) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    if towns and len(towns) > 0:
        # Converts to uppercase to handle case variations
        towns_uppercased = [town.upper() for town in towns]
        mask = df['town'].isin(towns_uppercased)
    return mask


def create_flat_types_mask(df: pd.DataFrame,
                          flat_types: Optional[List[str]] = None) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    if flat_types and len(flat_types) > 0:
        types_uppercased = [ft.upper() for ft in flat_types]
        mask = df['flat_type'].isin(types_uppercased)
    return mask


def create_floor_area_mask(df: pd.DataFrame,
                          min_area: Optional[float] = None,
                          max_area: Optional[float] = None) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    if min_area is not None:
        mask &= (df['floor_area_sqm'] >= min_area)
    if max_area is not None:
        mask &= (df['floor_area_sqm'] <= max_area)
    return mask


def create_storey_ranges_mask(df: pd.DataFrame,
                      storey_ranges: Optional[List[str]] = None) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    if storey_ranges and len(storey_ranges) > 0:
        ranges_uppercased = [sr.upper() for sr in storey_ranges]
        mask = df['storey_range'].isin(ranges_uppercased)
    return mask


def create_remaining_lease_mask(df: pd.DataFrame,
                               min_lease: Optional[float] = None) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    if min_lease is not None:
        mask = df['remaining_lease_years'] >= min_lease
    return mask



def create_flat_models_mask(df: pd.DataFrame,
                           flat_models: Optional[List[str]] = None) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    if flat_models and len(flat_models) > 0:
        models_uppercased = [model.upper() for model in flat_models]
        mask = df['flat_model'].isin(models_uppercased)
    return mask


def csp_filter_flats(df: pd.DataFrame,
                         constraints: Dict[str, Any],
                         verbose: bool = False) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Filters HDB flats by applying all CSP constraints sequentially.
    
    Args:
        df: Preprocessed HDB dataframe
        constraints: Dictionary of filtering constraints
        verbose: Print filtering statistics if True
        
    Returns:
        Tuple of (filtered_dataframe, statistics_dict)
    """
    start_time = time.time()
    initial_count = len(df)
    
    if verbose:
        print(f"Starting with {initial_count} flats")
    
    combined_mask = pd.Series([True] * len(df), index=df.index)
    
    # 1. Price constraint
    if 'min_price' in constraints or 'max_price' in constraints:
        price_mask = create_price_mask(
            df,
            constraints.get('min_price'),
            constraints.get('max_price')
        )
        combined_mask &= price_mask
        if verbose:
            print(f"After price filter: {combined_mask.sum()} flats remaining")
    
    # 2. Town constraint
    if 'towns' in constraints:
        town_mask = create_towns_mask(df, constraints['towns'])
        combined_mask &= town_mask
        if verbose:
            print(f"After town filter: {combined_mask.sum()} flats remaining")
    
    # 3. Flat type constraint
    if 'flat_types' in constraints:
        flat_type_mask = create_flat_types_mask(df, constraints['flat_types'])
        combined_mask &= flat_type_mask
        if verbose:
            print(f"After flat type filter: {combined_mask.sum()} flats remaining")

    # 4. Floor area constraint
    if 'min_floor_area' in constraints or 'max_floor_area' in constraints:
        floor_area_mask = create_floor_area_mask(
            df,
            constraints.get('min_floor_area'),
            constraints.get('max_floor_area')
        )
        combined_mask &= floor_area_mask
        if verbose:
            print(f"After floor area filter: {combined_mask.sum()} flats remaining")

    # 5. Storey range constraint
    if 'storey_ranges' in constraints:
        storey_ranges_mask = create_storey_ranges_mask(df, constraints['storey_ranges'])
        combined_mask &= storey_ranges_mask
        if verbose:
            print(f"After storey filter: {combined_mask.sum()} flats remaining")
    
    # 6. Remaining lease constraint
    if 'min_remaining_lease' in constraints:
        remaining_lease_mask = create_remaining_lease_mask(
            df,
            constraints['min_remaining_lease']
        )
        combined_mask &= remaining_lease_mask
        if verbose:
            print(f"After lease filter: {combined_mask.sum()} flats remaining")
    
    # 7. Flat model constraint
    if 'flat_models' in constraints:
        model_mask = create_flat_models_mask(df, constraints['flat_models'])
        combined_mask &= model_mask
        if verbose:
            print(f"After flat model filter: {combined_mask.sum()} flats remaining")
    
    df_filtered = df[combined_mask].copy()

    end_time = time.time()
    elapsed_time = end_time - start_time

    stats = get_filter_statistics(df, df_filtered)
    
    if verbose:
        print(f"\nFinal result: {len(df_filtered)} flats")
        print(f"Filtered out: {initial_count - len(df_filtered)} flats ({100 * (initial_count - len(df_filtered)) / initial_count:.1f}%)")
        print(f"Time taken: {elapsed_time:.4f} seconds")
    
    return df_filtered, stats


def get_filter_statistics(original_df: pd.DataFrame, 
                         filtered_df: pd.DataFrame) -> Dict[str, Any]:
    original_size = len(original_df)
    
    stats = {
        'total_results': len(filtered_df),
        'percentage_of_original': 100 * len(filtered_df) / original_size if original_size > 0 else 0,
        'price_range': {
            'min': filtered_df['resale_price'].min() if len(filtered_df) > 0 else None,
            'max': filtered_df['resale_price'].max() if len(filtered_df) > 0 else None,
            'median': filtered_df['resale_price'].median() if len(filtered_df) > 0 else None
        },
        'towns_present': sorted(filtered_df['town'].unique().tolist()) if len(filtered_df) > 0 else [],
        'flat_types_present': sorted(filtered_df['flat_type'].unique().tolist()) if len(filtered_df) > 0 else []
    }
    return stats



if __name__ == "__main__":
    test_data = pd.DataFrame({
        'town': ['BISHAN', 'ANG MO KIO', 'TAMPINES', 'BISHAN', 'QUEENSTOWN'],
        'flat_type': ['4 ROOM', '5 ROOM', '3 ROOM', '4 ROOM', '5 ROOM'],
        'resale_price': [450000, 550000, 300000, 480000, 600000],
        'floor_area_sqm': [95, 110, 75, 92, 120],
        'storey_range': ['01 TO 03', '07 TO 09', '04 TO 06', '04 TO 06', '10 TO 12'],
        'remaining_lease_years': [65, 70, 80, 60, 55],
        'flat_model': ['Model A', 'Improved', 'Standard', 'Model A', 'Premium']
    })
    print(test_data)
    
    test_constraints = {
        'min_price': 400000,
        'max_price': 550000,
        'towns': ['BISHAN', 'ANG MO KIO'],
        'flat_types': ['4 ROOM', '5 ROOM'],
        'min_floor_area': 90,
        'storey_ranges': ['04 TO 06', '07 TO 09']
    }
   
    resultdf, stats = csp_filter_flats(test_data, test_constraints, verbose=True)
    
    print("\n\nFiltered Results:")
    print(resultdf)
    
    print(f"Total results: {stats['total_results']}")
    print(f"Percentage of original: {stats['percentage_of_original']:.1f}%")
    print(f"Price range: ${stats['price_range']['min']} - ${stats['price_range']['max']}")
    