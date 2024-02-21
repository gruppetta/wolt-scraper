# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 14:18:38 2024

@author: grupp
"""

import pandas as pd
from settings_utils_functions import (
    session,
    user_agents,
    max_wait_time,
    scrape_with_retries_and_delay,
    compare_and_add_new_or_updated_items,
)

# Load URLs from CSV
urls_df = pd.read_csv("path/to/Restaurant_URL.csv")
existing_df = pd.read_csv("path/to/Rest_data_.csv")

# Convert your DataFrame column to a list
urls = urls_df['urls'].tolist()

# Start the scraping process
final_results = scrape_with_retries_and_delay(urls)

# Process scraped data
flattened_data = [item for entry in final_results if entry.get('data') for item in entry['data']]
scraped_df = pd.DataFrame(flattened_data)

# Apply the function to compare and update items
final_df = compare_and_add_new_or_updated_items(scraped_df, existing_df)

# Save the updated DataFrame
final_df.to_csv("path/to/updated_data.csv", index=False)