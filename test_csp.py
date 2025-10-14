import unittest
import pandas as pd
from modules.csp_filter import (
    create_price_mask,
    create_towns_mask,
    create_flat_types_mask,
    create_floor_area_mask,
    create_storey_ranges_mask,
    create_remaining_lease_mask,
    create_flat_models_mask,
    csp_filter_flats,
    get_filter_statistics
)

class TestCSPFilter(unittest.TestCase):
    
    def setUp(self):
        self.df = pd.DataFrame({
            'town': ['BISHAN', 'ANG MO KIO', 'TAMPINES', 'BISHAN', 'QUEENSTOWN', 'BISHAN'],
            'flat_type': ['EXECUTIVE', '5 ROOM', '3 ROOM', '4 ROOM', '5 ROOM', '4 ROOM'],
            'resale_price': [850000, 550000, 300000, 480000, 600000, 420000],
            'floor_area_sqm': [143, 110, 75, 92, 120, 88],
            'storey_range': ['01 TO 03', '07 TO 09', '04 TO 06', '04 TO 06', '10 TO 12', '07 TO 09'],
            'remaining_lease_years': [74, 70, 80, 60, 55, 68],
            'flat_model': ['APARTMENT', 'IMPROVED', 'STANDARD', 'MODEL A', 'PREMIUM', 'MODEL A']
        })
    
    def test_price_filter(self):
        mask = create_price_mask(self.df, min_price=400000, max_price=500000)
        self.assertEqual(mask.sum(), 2)
        
        mask = create_price_mask(self.df, min_price=500000)
        self.assertEqual(mask.sum(), 3)
        
        mask = create_price_mask(self.df)
        self.assertTrue(mask.all())
    
    def test_town_filter(self):
        mask = create_towns_mask(self.df, towns=['BISHAN'])
        self.assertEqual(mask.sum(), 3)
        
        mask = create_towns_mask(self.df, towns=['bishan'])
        self.assertEqual(mask.sum(), 3)
        
        mask = create_towns_mask(self.df, towns=['BISHAN', 'TAMPINES'])
        self.assertEqual(mask.sum(), 4)
    
    def test_flat_type_filter(self):
        mask = create_flat_types_mask(self.df, flat_types=['4 ROOM'])
        self.assertEqual(mask.sum(), 2)
        
        mask = create_flat_types_mask(self.df, flat_types=['4 ROOM', '5 ROOM'])
        self.assertEqual(mask.sum(), 4)
    
    def test_floor_area_filter(self):
        mask = create_floor_area_mask(self.df, min_area=90, max_area=120)
        self.assertEqual(mask.sum(), 3)
        
        mask = create_floor_area_mask(self.df, min_area=100)
        self.assertEqual(mask.sum(), 3)
    
    def test_storey_range_filter(self):
        mask = create_storey_ranges_mask(self.df, storey_ranges=['04 TO 06'])
        self.assertEqual(mask.sum(), 2)
        
        mask = create_storey_ranges_mask(self.df, storey_ranges=['04 TO 06', '07 TO 09'])
        self.assertEqual(mask.sum(), 4)
    
    def test_remaining_lease_filter(self):
        mask = create_remaining_lease_mask(self.df, min_lease=70)
        self.assertEqual(mask.sum(), 3)
    
    def test_flat_model_filter(self):
        mask = create_flat_models_mask(self.df, flat_models=['MODEL A'])
        self.assertEqual(mask.sum(), 2)
    
    def test_no_constraints(self):
        filtered_df, stats = csp_filter_flats(self.df, {})
        self.assertEqual(len(filtered_df), len(self.df))
        self.assertEqual(stats['total_results'], 6)
    
    def test_single_price_constraint(self):
        constraints = {'min_price': 400000, 'max_price': 500000}
        filtered_df, stats = csp_filter_flats(self.df, constraints)
        self.assertEqual(len(filtered_df), 2)
        self.assertTrue(all(filtered_df['resale_price'] >= 400000))
        self.assertTrue(all(filtered_df['resale_price'] <= 500000))
    
    def test_single_town_constraint(self):
        constraints = {'towns': ['BISHAN']}
        filtered_df, stats = csp_filter_flats(self.df, constraints)
        self.assertEqual(len(filtered_df), 3)
        self.assertTrue(all(filtered_df['town'] == 'BISHAN'))
    
    def test_multiple_constraints(self):
        constraints = {
            'min_price': 400000,
            'max_price': 500000,
            'towns': ['BISHAN'],
            'flat_types': ['4 ROOM']
        }
        filtered_df, stats = csp_filter_flats(self.df, constraints)
        self.assertEqual(len(filtered_df), 2)
        self.assertTrue(all(filtered_df['town'] == 'BISHAN'))
        self.assertTrue(all(filtered_df['flat_type'] == '4 ROOM'))
    
    def test_all_constraints(self):
        constraints = {
            'min_price': 400000,
            'max_price': 500000,
            'towns': ['BISHAN'],
            'flat_types': ['4 ROOM'],
            'min_floor_area': 90,
            'storey_ranges': ['04 TO 06'],
            'min_remaining_lease': 60,
            'flat_models': ['MODEL A']
        }
        filtered_df, stats = csp_filter_flats(self.df, constraints)
        self.assertEqual(len(filtered_df), 1)
        self.assertEqual(filtered_df.iloc[0]['resale_price'], 480000)
    
    def test_no_matching_results(self):
        constraints = {'min_price': 1000000}
        filtered_df, stats = csp_filter_flats(self.df, constraints)
        self.assertEqual(len(filtered_df), 0)
        self.assertEqual(stats['total_results'], 0)
    
    def test_statistics_calculation(self):
        filtered_df = self.df[self.df['town'] == 'BISHAN']
        stats = get_filter_statistics(self.df, filtered_df)
        
        self.assertEqual(stats['total_results'], 3)
        self.assertEqual(stats['percentage_of_original'], 50.0)
        self.assertIsNotNone(stats['price_range']['min'])
        self.assertIsNotNone(stats['price_range']['max'])
    
    def test_empty_filtered_results(self):
        empty_df = pd.DataFrame(columns=['town', 'flat_type', 'resale_price'])
        stats = get_filter_statistics(self.df, empty_df)
        
        self.assertEqual(stats['total_results'], 0)
        self.assertEqual(stats['percentage_of_original'], 0)
        self.assertIsNone(stats['price_range']['min'])
    
    def test_empty_dataframe(self):
        empty_df = pd.DataFrame(columns=['town', 'flat_type', 'resale_price', 
                                        'floor_area_sqm', 'storey_range', 
                                        'remaining_lease_years', 'flat_model'])
        constraints = {'min_price': 400000}
        filtered_df, stats = csp_filter_flats(empty_df, constraints)
        self.assertEqual(len(filtered_df), 0)


if __name__ == '__main__':
    unittest.main()