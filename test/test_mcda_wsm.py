import unittest
import pandas as pd
import numpy as np
from modules.mcda_wsm import normalize_column, mcda_wsm

class TestMCDA(unittest.TestCase):

    def setUp(self):
        """This method runs before each test. It sets up our test data."""
        self.df = pd.DataFrame({
            'resale_price': [400000, 500000, 600000],
            'floor_area_sqm': [80, 100, 120],
            'remaining_lease_years': [60, 75, 90]
        })
        
        self.criteria = {
            'resale_price': {'direction': 'cost'},
            'floor_area_sqm': {'direction': 'benefit'}
        }

    def test_normalize_column(self):
        """Tests the min-max normalization logic."""
        price_norm = normalize_column(self.df['resale_price'], 'cost')
        self.assertAlmostEqual(price_norm.iloc[0], 1.0)
        self.assertAlmostEqual(price_norm.iloc[1], 0.5)
        self.assertAlmostEqual(price_norm.iloc[2], 0.0)

        area_norm = normalize_column(self.df['floor_area_sqm'], 'benefit')
        self.assertAlmostEqual(area_norm.iloc[0], 0.0)
        self.assertAlmostEqual(area_norm.iloc[1], 0.5)
        self.assertAlmostEqual(area_norm.iloc[2], 1.0)
        
    def test_mcda_equal_weights(self):
        """Tests MCDA ranking when price and area are equally important."""
        ranked_df, _ = mcda_wsm(self.df, self.criteria, weights=None)
        
        self.assertAlmostEqual(ranked_df['score'].iloc[0], 5.0)
        self.assertAlmostEqual(ranked_df['score'].iloc[1], 5.0)
        self.assertAlmostEqual(ranked_df['score'].iloc[2], 5.0)
        self.assertEqual(ranked_df.iloc[0]['rank'], 1)

    def test_mcda_price_priority(self):
        """Tests MCDA ranking when price is the top priority."""
        price_priority_weights = {'resale_price': 0.8, 'floor_area_sqm': 0.2}
        ranked_df, _ = mcda_wsm(self.df, self.criteria, weights=price_priority_weights)

        top_ranked_price = ranked_df[ranked_df['rank'] == 1]['resale_price'].iloc[0]
        self.assertEqual(top_ranked_price, 400000)
        
        self.assertAlmostEqual(ranked_df.iloc[0]['score'], 8.0)
        self.assertAlmostEqual(ranked_df.iloc[2]['score'], 2.0)

    def test_mcda_area_priority(self):
        """Tests MCDA ranking when floor area is the top priority."""
        area_priority_weights = {'resale_price': 0.2, 'floor_area_sqm': 0.8}
        ranked_df, _ = mcda_wsm(self.df, self.criteria, weights=area_priority_weights)

        top_ranked_area = ranked_df[ranked_df['rank'] == 1]['floor_area_sqm'].iloc[0]
        self.assertEqual(top_ranked_area, 120)
        
        self.assertAlmostEqual(ranked_df.iloc[0]['score'], 8.0)
        self.assertAlmostEqual(ranked_df.iloc[2]['score'], 2.0)
        
    def test_empty_dataframe(self):
        """Tests that the function handles an empty DataFrame gracefully."""
        empty_df = pd.DataFrame(columns=['resale_price', 'floor_area_sqm'])
        ranked_df, _ = mcda_wsm(empty_df, self.criteria)
        self.assertTrue(ranked_df.empty)

if __name__ == '__main__':
    unittest.main()