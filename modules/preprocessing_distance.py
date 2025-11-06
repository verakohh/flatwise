"""
HDB Data Preprocessing Pipeline with Location Enrichment

This module handles the cleaning and preprocessing of HDB resale flat data,
including location enrichment using the OneMap API to fetch latitude, longitude, and
nearest MRT station.
"""

import pandas as pd
import numpy as np
import os
import json
import requests
import time
from typing import Tuple, Dict, Any, Optional
from geopy.distance import great_circle
from dotenv import load_dotenv 

load_dotenv()

ONEMAP_API_TOKEN = os.environ.get("ONEMAP_API_TOKEN")
ONEMAP_SEARCH_URL = "https://www.onemap.gov.sg/api/common/elastic/search"
ONEMAP_MRT_URL = "https://www.onemap.gov.sg/api/public/nearbysvc/getNearestMrtStops"
API_DELAY_SEC = float(os.environ.get("API_DELAY_SEC", "0.25"))
CACHE_FILE = os.environ.get("CACHE_FILE", "data/location_cache.json")
REQUEST_TIMEOUT = 10

# Validate API token
if not ONEMAP_API_TOKEN:
    print("WARNING: ONEMAP_API_TOKEN not found!")

STREET_ABBREVIATIONS = {
    'AVE': 'AVENUE', 'ST': 'STREET', 'RD': 'ROAD', 'JLN': 'JALAN',
    'LOR': 'LORONG', 'BLVD': 'BOULEVARD', 'CL': 'CLOSE', 'CRES': 'CRESCENT',
    'CT': 'COURT', 'DR': 'DRIVE', 'GR': 'GROVE', 'LK': 'LINK',
    'PL': 'PLACE', 'PK': 'PARK', 'SQ': 'SQUARE', 'TER': 'TERRACE',
    'TG': 'TANJONG', 'BT': 'BUKIT', 'UPP': 'UPPER', 'CTRL': 'CENTRAL',
    'NTH': 'NORTH', 'STH': 'SOUTH', 'EST': 'ESTATE',
    "C'WEALTH": 'COMMONWEALTH', 'CWEALTH': 'COMMONWEALTH',
}

class OneMapAPIError(Exception):
    """Custom exception for OneMap API errors."""
    pass

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
        if lowered_part == 'years' and i > 0 and parts[i-1].replace('.', '', 1).isdigit():
            years = float(parts[i-1])
        elif lowered_part == 'months' and i > 0 and parts[i-1].replace('.', '', 1).isdigit():
            months = float(parts[i-1])

    total_years = years + (months / 12.0)
    return round(total_years, 2)


def validate_storey_range_format(storey_range: str) -> bool:
    """
    Validates storey range follows the expected 'XX TO YY' format.
    """
    if pd.isna(storey_range):
        return False
    
    try:
        parts = str(storey_range).strip().split(' TO ')
        if len(parts) != 2:
            return False
        
        lower, upper = parts[0].strip(), parts[1].strip()
        if not (lower.isdigit() and upper.isdigit() and 
                len(lower) == 2 and len(upper) == 2):
            return False
        
        if int(upper) <= int(lower):
            return False
        
        return True
    except (ValueError, AttributeError):
        return False


def normalise_street_name(street_name: str) -> str:
    """Standardises street names by expanding abbreviations."""
    if pd.isna(street_name):
        return ""
    
    name = str(street_name).strip().upper()
    words = name.split()
    expanded_words = [STREET_ABBREVIATIONS.get(word, word) for word in words]
    return ' '.join(expanded_words)


def load_cache(cache_path: str) -> Dict[str, Any]:
    """Load cached API results from JSON file."""
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                cache = json.load(f)
                print(f"Loaded {len(cache)} cached locations from {cache_path}")
                return cache
        except json.JSONDecodeError:
            print("Warning: Cache file corrupted. Starting fresh.")
            return {}
    return {}


def save_cache(cache: Dict[str, Any], cache_path: str):
    os.makedirs(os.path.dirname(cache_path) if os.path.dirname(cache_path) else '.', 
                exist_ok=True)
    with open(cache_path, 'w') as f:
        json.dump(cache, f, indent=2)


def geocode_address(block: str, street: str) -> Optional[Tuple[float, float]]:
    """
    Converts HDB block and street to geographic coordinates.
    
    Args:
        block: HDB block number
        street: Normalised street name
    
    Returns:
        Tuple of (latitude, longitude) or None if geocoding fails
    
    Raises:
        OneMapAPIError: If API request fails after retries
    """
    full_address = f"{block} {street}"
    
    headers = {}
    if ONEMAP_API_TOKEN:
        headers['Authorization'] = ONEMAP_API_TOKEN
    
    search_params = {
        'searchVal': full_address,
        'returnGeom': 'Y',
        'getAddrDetails': 'Y'
    }
    
    for attempt in range(3):
        try:
            response = requests.get(
                ONEMAP_SEARCH_URL,
                params=search_params,
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            
            if not data or not data.get('results'):
                return None
            
            result = data['results'][0]
            lat = float(result['LATITUDE'])
            lon = float(result['LONGITUDE'])
            
            return (lat, lon)
        
        except requests.exceptions.RequestException as e:
            if attempt < 2:
                delay = API_DELAY_SEC * (2 ** attempt)
                time.sleep(delay)
            else:
                raise OneMapAPIError(f"Geocoding failed: {str(e)}")
    
    return None

def find_nearest_mrt(lat: float, lon: float, radius_m: int = 2000) -> Optional[Dict[str, Any]]: 
    """
    Finds the nearest MRT station within a specified radius.
    
    Args:
        lat: Latitude of the location
        lon: Longitude of the location
        radius_m: Search radius in meters (default: 2000)
    
    Returns:
        Dictionary with nearest MRT station information or None if not found.
    
    Raises:
        OneMapAPIError: If API request fails
    """
    headers = {}
    if ONEMAP_API_TOKEN:
        headers['Authorization'] = ONEMAP_API_TOKEN

    nearby_params = {
        'latitude': lat,
        'longitude': lon,
        'radius_in_meters': radius_m,
    }

    try:
        response = requests.get(
            ONEMAP_MRT_URL,
            params=nearby_params,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()

        if not data or not isinstance(data, list) or len(data) == 0:
            return None
        flat_coords = (lat, lon)        
        nearest_mrt_dist = float('inf')
        nearest_mrt_name = None

        for station in data:
            station_coords = (float(station['lat']), float(station['lon']))
            dist_km = great_circle(flat_coords, station_coords).km

            if dist_km < nearest_mrt_dist:
                nearest_mrt_dist = dist_km
                nearest_mrt_name = station['name']
        if nearest_mrt_name:
            return {
                'name': nearest_mrt_name,
                'distance_km': round(nearest_mrt_dist, 3)
            }
        
        return None
            
    except requests.exceptions.RequestException as e:
        raise OneMapAPIError(f"MRT lookup failed: {str(e)}")

def get_mrt_with_retry(lat: float, lon: float) -> Dict[str, Any]:
    """
    Multi-tier MRT search: 2km then 5km then None.
    
    Args:
        lat: Latitude coordinate
        lon: Longitude coordinate
    
    Returns:
        Dictionary with MRT data and search metadata
    """
    # Tier 1: Try 2km radius (most common case)
    try:
        mrt_result = find_nearest_mrt(lat, lon, radius_m=2000)
        if mrt_result:
            return {
                'nearest_mrt': mrt_result['name'],
                'dist_mrt_km': mrt_result['distance_km'],
                'search_radius_km': 2.0
            }
    except OneMapAPIError:
        pass
    
    # Small delay between attempts
    time.sleep(API_DELAY_SEC * 0.5)
    
    # Tier 2: Extend to 5km radius
    try:
        mrt_result = find_nearest_mrt(lat, lon, radius_m=5000)
        if mrt_result:
            return {
                'nearest_mrt': mrt_result['name'],
                'dist_mrt_km': mrt_result['distance_km'],
                'search_radius_km': 5.0
            }
    except OneMapAPIError:
        pass
    
    # Tier 3: No MRT found within 5km
    return {
        'nearest_mrt': np.nan,
        'dist_mrt_km': np.nan,
        'search_radius_km': np.nan
    }
def get_location_data_from_onemap(block: str, street: str) -> Optional[Dict[str, Any]]:
    """
    Fetches complete location data using OneMap API.
    
     Args:
        block: HDB block number
        street: Normalised street name
        
    Returns:
        Dictionary with location data:
        {
            'block': str,
            'street_name': str,
            'latitude': float,
            'longitude': float,
            'nearest_mrt': str,
            'dist_mrt_km': float
        }
        Returns None if API calls fail after retries.
    """
     # Step 1: Geocode address
    try:
        coords = geocode_address(block, street)
        if not coords:
            return None
        
        lat, lon = coords
        
    except OneMapAPIError:
        return None
    
    time.sleep(API_DELAY_SEC)
    
    # Step 2: Find MRT with retry strategy
    mrt_data = get_mrt_with_retry(lat, lon)
    
    return {
        'block': block,
        'street_name': street,
        'latitude': lat,
        'longitude': lon,
        **mrt_data
    }


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
    for col in ['town', 'flat_type', 'flat_model', 'storey_range', 'street_name', 'block']:
        if col in clean_df.columns:
            clean_df[col] = clean_df[col].astype(str).str.upper()

    # Ensures numeric columns are numeric
    for col in ['floor_area_sqm', 'lease_commence_date', 'resale_price']:
        if col in clean_df.columns:
            clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce')
    
    if 'month' in clean_df.columns:
        clean_df = clean_df.rename(columns={'month': 'transaction_date'})
    
    if 'remaining_lease' in clean_df.columns:
        clean_df['remaining_lease_years'] = clean_df['remaining_lease'].apply(
            extract_remaining_lease_years
        )
    
    print(f"Core cleaning complete. Rows: {len(clean_df)}")
    return clean_df


def enrich_with_location_data(df: pd.DataFrame, cache_path: str = CACHE_FILE) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Enriches dataset with location features using cached OneMap API calls."""
    print("\n" + "="*60)
    print("Location Enrichment Pipeline")
    print("="*60)

    print("\nStep 1: Normalising street names")
    df['clean_street_name'] = df['street_name'].apply(normalise_street_name)
    df['address_key'] = df['block'].astype(str) + ' ' + df['clean_street_name']

    unique_addresses = df[['block', 'clean_street_name', 'address_key']]\
        .drop_duplicates()\
        .reset_index(drop=True)

    print(f"Total unique addresses: {len(unique_addresses)}")

    location_cache = load_cache(cache_path)
    processed_keys = set(location_cache.keys())

    print("\nStep 2: Merging with pre-existing coordinate data")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    zipcode_path = os.path.join(script_dir, 'data', 'sg_zipcode_mapper.csv')

    hdb_coordinates = pd.read_csv(zipcode_path, encoding='latin-1')
    hdb_coordinates['address_key'] = hdb_coordinates['blk_no'].astype(str) + ' ' + hdb_coordinates['road_name']
    df_merge = unique_addresses.merge(hdb_coordinates[['address_key', 'latitude', 'longitude']], on='address_key', how='left')

    addresses_with_coords = df_merge[
        (df_merge['latitude'].notnull()) &
        (df_merge['longitude'].notnull()) &
        (~df_merge['address_key'].isin(processed_keys))
    ].copy()

    addresses_without_coords = df_merge[
        ((df_merge['latitude'].isnull()) | (df_merge['longitude'].isnull())) &
        (~df_merge['address_key'].isin(processed_keys))
    ].copy()

    already_cached = len(unique_addresses) - len(addresses_with_coords) - len(addresses_without_coords)

    print(f"Already in cache: {already_cached:,}")
    print(f"Have coordinates (need MRT only): {len(addresses_with_coords):,}")
    print(f"Missing coordinates (need geocode + MRT): {len(addresses_without_coords):,}")

    total_api_calls_needed = len(addresses_with_coords) + (len(addresses_without_coords) * 2)
    api_calls_saved = len(addresses_with_coords)
    print(f"\nAPI Call Optimisation:")
    print(f"  Total API calls needed: {total_api_calls_needed:,}")
    print(f"  API calls saved by using pre-existing coordinates: {api_calls_saved:,}")

    stats = {
        'total_processed': 0,
        'has_coords': 0,
        'no_coords': 0,
        'geocode_success': 0,
        'geocode_failed': 0,
        'mrt_2km': 0,
        'mrt_5km': 0,
        'no_mrt': 0,
        'api_calls_saved': api_calls_saved
    }

    total_to_process = len(addresses_with_coords) + len(addresses_without_coords)

    if total_to_process > 0:
        print("\nStep 3: Fetching location data")
        print(f"Estimated time: ~{(len(addresses_with_coords) * 0.5 + len(addresses_without_coords) * 1.5):.0f} seconds")

        if len(addresses_with_coords) > 0:
            print(f"\nProcessing {len(addresses_with_coords)} addresses with existing coordinates (MRT lookup only)...")

            for row in addresses_with_coords.itertuples():
                stats['total_processed'] += 1
                stats['has_coords'] += 1

                if stats['total_processed'] % 50 == 1:
                    print(f"Progress: {stats['total_processed']}/{total_to_process} | "
                          f"Has coords: {stats['has_coords']} | Need geocode: {stats['no_coords']} | "
                          f"MRT 2km: {stats['mrt_2km']} | 2-5km: {stats['mrt_5km']} | Failures: {stats['geocode_failed']}")

                lat, lon = row.latitude, row.longitude
                mrt_data = get_mrt_with_retry(lat, lon)

                result = {
                    'block': row.block,
                    'street_name': row.clean_street_name,
                    'latitude': lat,
                    'longitude': lon,
                    **mrt_data
                }

                location_cache[row.address_key] = result
                stats['geocode_success'] += 1

                if pd.notna(result.get('search_radius_km')):
                    if result['search_radius_km'] == 2.0:
                        stats['mrt_2km'] += 1
                    elif result['search_radius_km'] == 5.0:
                        stats['mrt_5km'] += 1
                else:
                    stats['no_mrt'] += 1

                if stats['total_processed'] % 100 == 0:
                    save_cache(location_cache, cache_path)
                    print(f"  Intermediate save: {len(location_cache)} addresses cached")

                time.sleep(API_DELAY_SEC)

        if len(addresses_without_coords) > 0:
            print(f"\nProcessing {len(addresses_without_coords)} addresses without coordinates (geocode + MRT)...")

            for row in addresses_without_coords.itertuples():
                stats['total_processed'] += 1
                stats['no_coords'] += 1

                if stats['total_processed'] % 50 == 1:
                    print(f"Progress: {stats['total_processed']}/{total_to_process} | "
                          f"Has coords: {stats['has_coords']} | Need geocode: {stats['no_coords']} | "
                          f"MRT 2km: {stats['mrt_2km']} | 2-5km: {stats['mrt_5km']} | Failures: {stats['geocode_failed']}")

                result = get_location_data_from_onemap(row.block, row.clean_street_name)

                if result:
                    location_cache[row.address_key] = result
                    stats['geocode_success'] += 1

                    if pd.notna(result.get('search_radius_km')):
                        if result['search_radius_km'] == 2.0:
                            stats['mrt_2km'] += 1
                        elif result['search_radius_km'] == 5.0:
                            stats['mrt_5km'] += 1
                    else:
                        stats['no_mrt'] += 1
                else:
                    stats['geocode_failed'] += 1

                if stats['total_processed'] % 100 == 0:
                    save_cache(location_cache, cache_path)
                    print(f"  Intermediate save: {len(location_cache)} addresses cached")

                time.sleep(API_DELAY_SEC)

        save_cache(location_cache, cache_path)
        print(f"\nCompleted fetching. Total cached: {len(location_cache):,}")
    
    print("\nStep 4: Merging location data with dataset")
    cache_df = pd.DataFrame(location_cache.values())
    
    if len(cache_df) > 0:
        cache_df['address_key'] = (
            cache_df['block'].astype(str) + ' ' + cache_df['street_name']
        )
        
        location_features = cache_df[[
            'address_key', 'latitude', 'longitude',
            'nearest_mrt', 'dist_mrt_km', 'search_radius_km'
        ]].drop_duplicates()
    
        cols_to_drop = ['latitude', 'longitude', 'nearest_mrt', 'dist_mrt_km', 'search_radius_km'] 
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')
        
        final_df = df.merge(location_features, on='address_key', how='left')
        final_df = final_df.drop(columns=['clean_street_name', 'address_key'])
    else:
        print("Warning: No location data available")
        final_df = df
    
    # Calculate final statistics
    print("\n" + "="*60)
    print("MRT COVERAGE STATISTICS")
    print("="*60)
    
    total_properties = len(final_df)
    has_mrt = final_df['dist_mrt_km'].notna().sum()
    mrt_2km = (final_df['search_radius_km'] == 2.0).sum()
    mrt_5km = (final_df['search_radius_km'] == 5.0).sum()
    no_mrt = final_df['dist_mrt_km'].isna().sum()
    
    print(f"\nTotal properties: {total_properties:,}")
    print(f"Has MRT data: {has_mrt:,} ({has_mrt/total_properties*100:.2f}%)")
    print(f"  Within 2km: {mrt_2km:,} ({mrt_2km/total_properties*100:.2f}%)")
    print(f"  2-5km away: {mrt_5km:,} ({mrt_5km/total_properties*100:.2f}%)")
    print(f"No MRT within 5km: {no_mrt:,} ({no_mrt/total_properties*100:.2f}%)")
    
    if has_mrt > 0:
        print(f"\nMRT Distance Statistics (for properties with MRT):")
        print(final_df['dist_mrt_km'].describe())

    print("\n" + "="*60)
    print("API Call Optimisation Summary")
    print("="*60)
    print(f"Addresses with pre-existing coordinates: {stats.get('has_coords', 0):,}")
    print(f"Addresses that needed geocoding: {stats.get('no_coords', 0):,}")
    print(f"API calls saved by using pre-existing data: {stats.get('api_calls_saved', 0):,}")
    total_calls_made = stats.get('has_coords', 0) + (stats.get('no_coords', 0) * 2)
    total_calls_without_optimisation = (stats.get('has_coords', 0) + stats.get('no_coords', 0)) * 2
    if total_calls_without_optimisation > 0:
        reduction_pct = (stats.get('api_calls_saved', 0) / total_calls_without_optimisation) * 100
        print(f"Total API calls made: {total_calls_made:,}")
        print(f"Total API calls without optimisation: {total_calls_without_optimisation:,}")
        print(f"Reduction: {reduction_pct:.1f}%")

    # Compile statistics for report
    coverage_stats = {
        'total_properties': int(total_properties),
        'optimisation_metrics': {
            'had_existing_coords': int(stats.get('has_coords', 0)),
            'needed_geocoding': int(stats.get('no_coords', 0)),
            'api_calls_saved': int(stats.get('api_calls_saved', 0)),
            'total_api_calls_made': int(stats.get('has_coords', 0) + stats.get('no_coords', 0) * 2)
        },
        'geocode_success': int(stats.get('geocode_success', 0)),
        'geocode_failed': int(stats.get('geocode_failed', 0)),
        'mrt_coverage': {
            'total_has_mrt': int(has_mrt),
            'coverage_percentage': float(has_mrt/total_properties*100),
            'within_2km': int(mrt_2km),
            'between_2_5km': int(mrt_5km),
            'no_mrt_within_5km': int(no_mrt),
            'no_mrt_percentage': float(no_mrt/total_properties*100)
        },
        'distance_statistics': {
            'median_km': float(final_df['dist_mrt_km'].median()) if has_mrt > 0 else None,
            'mean_km': float(final_df['dist_mrt_km'].mean()) if has_mrt > 0 else None,
            'min_km': float(final_df['dist_mrt_km'].min()) if has_mrt > 0 else None,
            'max_km': float(final_df['dist_mrt_km'].max()) if has_mrt > 0 else None
        }
    }
    
    print(f"\nEnrichment complete. Final rows: {len(final_df):,}")
    
    return final_df, coverage_stats


def get_processing_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """Generates summary statistics."""
    return {
        'dataset_info': {
            'total_records': int(len(df)),
            'date_range': {
                'earliest': str(df['transaction_date'].min()) if 'transaction_date' in df else None,
                'latest': str(df['transaction_date'].max()) if 'transaction_date' in df else None
            }
        },
        'price_statistics': {
            'min': float(df['resale_price'].min()),
            'max': float(df['resale_price'].max()),
            'median': float(df['resale_price'].median()),
            'mean': float(df['resale_price'].mean()),
            'std': float(df['resale_price'].std())
        },
        'property_distribution': {
            'unique_towns': int(df['town'].nunique()),
            'unique_flat_types': int(df['flat_type'].nunique()),
            'towns': sorted(df['town'].unique().tolist()),
            'flat_types': sorted(df['flat_type'].unique().tolist())
        }
    }


def preprocess_hdb_data(
    filepath: str,
    include_location: bool = True,
    test_mode: bool = False,
    test_rows: int = 1000
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Complete preprocessing pipeline.
    
    Args:
        filepath: Path to raw CSV file
        include_location: Whether to enrich with location data
        test_mode: If True, process only first N rows
        test_rows: Number of rows for test mode
    
    Returns:
        Tuple of (processed DataFrame, statistics dictionary)
    """
    print("\n" + "="*60)
    print("HDB DATA PREPROCESSING PIPELINE")
    print("="*60)
    
    if test_mode:
        print(f"\nTEST MODE: Processing first {test_rows} rows only")
    
    print(f"\nLoading data from: {filepath}")
    
    if test_mode:
        df = pd.read_csv(filepath, nrows=test_rows)
    else:
        # Process in chunks for memory efficiency
        print("Loading in chunks for memory efficiency...")
        chunks = []
        chunk_size = 50000
        for i, chunk in enumerate(pd.read_csv(filepath, chunksize=chunk_size)):
            chunks.append(chunk)
            if (i + 1) % 5 == 0:
                print(f"  Loaded {(i + 1) * chunk_size:,} rows...")
        df = pd.concat(chunks, ignore_index=True)
    
    print(f"Loaded {len(df):,} rows and {len(df.columns)} columns")
    
    # Core cleaning
    df_cleaned = clean_data(df)
    
    # Location enrichment
    if include_location:
        df_enriched, coverage_stats = enrich_with_location_data(df_cleaned)
    else:
        print("\nSkipping location enrichment")
        df_enriched = df_cleaned
        coverage_stats = {}
    
    # Generate summary
    summary = get_processing_summary(df_enriched)
    if coverage_stats:
        summary['mrt_coverage'] = coverage_stats
    
    # Print final summary
    print("\n" + "="*60)
    print("PROCESSING COMPLETE")
    print("="*60)
    print(f"\nFinal dataset: {summary['dataset_info']['total_records']:,} records")
    print(f"Price range: ${summary['price_statistics']['min']:,.0f} - "
          f"${summary['price_statistics']['max']:,.0f}")
    print(f"Median price: ${summary['price_statistics']['median']:,.0f}")
    print(f"Unique towns: {summary['property_distribution']['unique_towns']}")
    print(f"Unique flat types: {summary['property_distribution']['unique_flat_types']}")
    
    return df_enriched, summary


if __name__ == "__main__":
    import sys
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(script_dir, '..', 'ResaleFlatPricesData.csv')
    
    # Check for test mode
    test_mode = len(sys.argv) > 1 and sys.argv[1].lower() == 'test'
    
    if test_mode:
        print("\n" + "="*60)
        print("TEST MODE - Processing 1000 rows")
        print("="*60)
        output_path = os.path.join(script_dir, '..', 'ResaleFlatPricesData_test.csv')
        stats_path = 'test_statistics.json'
    else:
        print("\n" + "="*60)
        print("FULL PROCESSING MODE")
        print("="*60)
        output_path = os.path.join(script_dir, '..', 'ResaleFlatPricesData_processed.csv')
        stats_path = 'processing_statistics.json'
    
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    
    # Run pipeline
    df_final, summary = preprocess_hdb_data(
        input_path, 
        test_mode=test_mode,
        test_rows=1000
    )
    
    # Save outputs
    print(f"\nSaving processed data to: {output_path}")
    df_final.to_csv(output_path, index=False)
    
    print(f"Saving statistics to: {stats_path}")
    with open(stats_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print("\n" + "="*60)
    print("Preprocessing complete")
    print("="*60)
    