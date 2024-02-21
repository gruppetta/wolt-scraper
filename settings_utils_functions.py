# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 14:14:29 2024

@author: grupp
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import hashlib
import re
import time
import random


def fetch_and_parse_url(url, attempt=1, max_attempts=2):
    
    """
    Attempts to fetch content from a specified URL and parse it into a BeautifulSoup object.
    Implements exponential backoff in case of a 429 (Too Many Requests) status code and retries up to a maximum number of attempts.
    
    Parameters:
    - url (str): The URL to fetch.
    - attempt (int): The current attempt number (default is 1).
    - max_attempts (int): The maximum number of attempts to make (default is 2).
    
    Returns:
    - tuple: A tuple containing a BeautifulSoup object of the parsed page and None if successful, or None and an error message string if an error occurred.
    """
    headers = {
        "User-Agent": random.choice(user_agents)
    }
    try:
        response = session.get(url, headers=headers)
        if response.status_code == 200:
            return BeautifulSoup(response.text, 'html.parser'), None
        elif response.status_code == 429:
            if attempt <= max_attempts:
                wait_time = min((2 ** attempt) + random.uniform(0, 1), max_wait_time)  # Exponential backoff with randomness
                time.sleep(wait_time)
                return fetch_and_parse_url(url, attempt + 1, max_attempts)  # Recursive call with increased attempt count
            else:
                return None, f"HTTP error 429: Too Many Requests. Max attempts reached."
        else:
            return None, f"HTTP error {response.status_code}"
    except Exception as e:
        return None, str(e)




def generate_simple_uuid(restaurant_name, item_name):
    """
    Generates a simple UUID for an item by hashing the concatenation of the restaurant name and item name.
    
    Parameters:
    - restaurant_name (str): The name of the restaurant.
    - item_name (str): The name of the item.
    
    Returns:
    - str: A hexadecimal MD5 hash of the restaurant and item names combined.
    """
    unique_string = f"{restaurant_name}{item_name}"
    return hashlib.md5(unique_string.encode('utf-8')).hexdigest()


def extract_uuid_from_url(url):
    """
    Extracts a UUID from the given URL.

    Parameters:
    - url (str): The URL from which to extract the UUID.

    Returns:
    - str: The extracted UUID if found, otherwise None.
    
    This function uses regular expression searching to find a UUID pattern within the URL.
    The expected UUID format is a 32-character hexadecimal string segmented by hyphens into 5 groups:
    8-4-4-4-12. If no such pattern is found, the function returns None.
    """
    match = re.search(r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})", url)
    return match.group(0) if match else None


def scrape_items_from_section_url(url):
    """
    Scrapes item details from a specific section URL.

    This function fetches the page content for the given URL, parses it to extract details of each item listed on the page,
    and returns a list of dictionaries with item data. If fetching or parsing fails, it returns an empty list and an error message.

    Parameters:
    - url (str): The URL of the section to scrape.

    Returns:
    - tuple: A tuple containing a list of item dictionaries and an error message if applicable. If successful, the error message is None.
    """
    soup, error_message = fetch_and_parse_url(url)
    
    if soup is None:  # If fetching failed, return an empty list and the error message
        return [], error_message
    
    page_title = soup.title.string if soup.title else 'Unknown Restaurant'
    restaurant_name = page_title.split('|')[0].strip()

    items_data = []
    items = soup.find_all('div', {'data-test-id': 'horizontal-item-card'})

    for item in items:
        name = item.find('h3', {'data-test-id': 'horizontal-item-card-header'}).get_text(strip=True) if item.find('h3') else 'Unknown'
        description = item.find('p', class_='sc-d8a5d6bd-0 bNvRem').get_text(strip=True) if item.find('p', class_='sc-d8a5d6bd-0 bNvRem') else ''

        discounted_price, original_price = extract_prices(item)

        image_url = item.find('img')['src'] if item.find('img') else ''
        uuid = extract_uuid_from_url(image_url) if image_url else generate_simple_uuid(restaurant_name, name)

        items_data.append({
            'Restaurant': restaurant_name,
            'Name': name,
            'Description': description,
            'DiscountedPrice': str(discounted_price) if discounted_price is not None else '',
            'OriginalPrice': str(original_price) if original_price is not None else '',
            'UUID': uuid,
            'ScrapedDateTime': datetime.now().strftime('%Y-%m-%d %H:%M')
        })

    return items_data, None

def extract_prices(item):
    """
    Extracts and cleans item prices from a BeautifulSoup tag.

    This helper function is used within `scrape_items_from_section_url` to clean and extract the original and discounted prices
    from the item's HTML data.

    Parameters:
    - item (BeautifulSoup tag): The HTML tag containing the item details.

    Returns:
    - tuple: A tuple containing the discounted price and the original price (both as floats or None if not available).
    """
    # Function to clean and extract price
    def clean_and_extract_price(price_span):
        if price_span:
            price_text = price_span.get_text(strip=True).replace('€', '').replace(',', '')
            try:
                return float(price_text)
            except ValueError:
                return None
        else:
            return None

    discounted_price_span = item.find('span', {'data-test-id': 'horizontal-item-card-discounted-price'})
    original_price_span = item.find('span', {'data-test-id': 'horizontal-item-card-original-price'})
    single_price_span = item.find('span', {'data-test-id': 'horizontal-item-card-price'})

    discounted_price = clean_and_extract_price(discounted_price_span)
    original_price = clean_and_extract_price(original_price_span)
    if single_price_span and discounted_price is None and original_price is None:
        discounted_price = clean_and_extract_price(single_price_span)
        original_price = None

    return discounted_price, original_price


def scrape_url_and_collect_data(url, delay=0):
    """
    Scrapes data from a given URL with an optional delay before making the request.
    This function is designed to mitigate potential rate-limiting issues by introducing a delay.
    It attempts to scrape data from the provided URL and returns the outcome of the scrape attempt.

    Parameters:
    - url (str): The URL to scrape.
    - delay (int): The delay in seconds before the request is made (default is 0).

    Returns:
    - dict: A dictionary containing the URL, a success flag, and either the scraped data or an error message.
    """
    if delay > 0:
        time.sleep(delay)  # Delay the request if specified
    items, error_message = scrape_items_from_section_url(url)
    return {
        'url': url,
        'success': False if error_message else True,
        'data': items if not error_message else None,
        'error': error_message if error_message else None
    }

from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def scrape_with_retries_and_delay(urls, max_retries=4):
    """
    Scrapes a list of URLs with retries and optional delays, using concurrent requests to optimize performance.
    Retries are attempted for URLs that fail to be scraped successfully, up to a maximum number of retries.

    Parameters:
    - urls (list of str): A list of URLs to be scraped.
    - max_retries (int): The maximum number of retries for each URL (default is 4).

    Returns:
    - list of dict: A list of dictionaries, each representing the result of a scrape attempt for a URL.
    """
    attempt_counts = {url: 1 for url in urls}
    results = []
    urls_to_scrape = urls.copy()

    while urls_to_scrape:
        print(f"Scraping round with {len(urls_to_scrape)} URLs...")
        new_results, new_urls_to_scrape = [], []

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(scrape_url_and_collect_data, url, 1 if attempt_counts[url] == max_retries else 0): url for url in urls_to_scrape}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Scraping"):
                result = future.result()
                new_results.append(result)
                if not result['success'] and attempt_counts[futures[future]] < max_retries:
                    new_urls_to_scrape.append(futures[future])
                    attempt_counts[futures[future]] += 1

        urls_to_scrape = new_urls_to_scrape
        results.extend(new_results)
        print(f"Round completed. Successful: {len([r for r in results if r['success']])}, Unsuccessful: {len(results) - len([r for r in results if r['success']])}")

    print(f"Total unscraped URLs after all attempts: {len([r for r in results if not r['success']])}")
    return results


from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def scrape_with_retries_and_delay(urls, max_retries=4):
    """
    Scrapes a list of URLs with retries and optional delays, employing concurrent requests to optimize performance.
    This function attempts retries for URLs that fail to be scraped successfully, up to a specified maximum number of retries.
    It introduces a delay on the last retry attempt to mitigate potential rate-limiting by the server.

    Parameters:
    - urls (list of str): The list of URLs to be scraped.
    - max_retries (int): The maximum number of retries for each URL. Default is 4.

    Returns:
    - list of dict: A list of dictionaries, each representing the scraping result for a URL. Each dictionary contains the URL,
      a success flag indicating whether the scrape was successful, the scraped data (if any), and an error message (if any).
    
    The function utilizes a ThreadPoolExecutor for concurrent scraping, improving efficiency when scraping multiple URLs.
    After each round of attempts, it prints the count of successful and unsuccessful scrapes, providing a summary at the end.
    """
    attempt_counts = {url: 1 for url in urls}  # Initialize attempt counts for each URL
    results = []
    urls_to_scrape = urls.copy()  # Create a mutable copy of the URLs list for modification during retries
    
    while urls_to_scrape:
        print(f"Scraping round with {len(urls_to_scrape)} URLs...")
        new_results, new_urls_to_scrape = [], []

        with ThreadPoolExecutor(max_workers=10) as executor:  # Limit the number of concurrent threads
            futures = []
            for url in urls_to_scrape:
                delay = 1 if attempt_counts[url] == max_retries else 0  # Apply delay on the last retry attempt
                futures.append(executor.submit(scrape_url_and_collect_data, url, delay))  # Schedule the scraping task
                
            for future in tqdm(as_completed(futures), total=len(futures), desc="Scraping"):
                result = future.result()  # Retrieve the result from the future
                new_results.append(result)
                if not result['success'] and attempt_counts[result['url']] < max_retries:
                    new_urls_to_scrape.append(result['url'])  # Schedule for retry if not successful
                    attempt_counts[result['url']] += 1  # Increment the attempt count

        urls_to_scrape = new_urls_to_scrape  # Update the list of URLs to scrape in the next round
        results.extend(new_results)  # Aggregate results from this round

        # Print the progress summary for the current round
        successful_scrapes = [result for result in results if result['success']]
        print(f"Round completed. Successful: {len(successful_scrapes)}, Unsuccessful: {len(results) - len(successful_scrapes)}")

    # Summarize the total number of unscraped URLs after all attempts
    total_unscraped = len([result for result in results if not result['success']])
    print(f"Total unscraped URLs after all attempts: {total_unscraped}")

    return results




import pandas as pd

def compare_and_add_new_or_updated_items(scraped_df, existing_df):
    """
    Compares newly scraped data with existing data to identify new items or items with updated prices.
    It normalizes restaurant names and item names for comparison, merges the new data with existing data,
    and appends only new or updated items to the existing dataset, preserving historical data.

    Parameters:
    - scraped_df (pd.DataFrame): The DataFrame containing newly scraped data.
    - existing_df (pd.DataFrame): The DataFrame containing existing data.

    Returns:
    - pd.DataFrame: A DataFrame containing the updated dataset with new or updated items appended,
                    alongside the existing data, ensuring no historical data is overwritten or removed.
    
    The function performs normalization of the 'Restaurant' and 'Name' columns to ensure accurate comparisons
    by eliminating case sensitivity and leading/trailing whitespace differences. It then merges the new data
    with the existing data based on these normalized fields. Items are considered new or updated based on
    their presence solely in the new data or differences in their 'DiscountedPrice' values. The final dataset
    includes all existing items plus any new or updated items, with duplicates removed to maintain data integrity.
    """

    # Normalize 'Restaurant' and 'Name' for comparison (e.g., lowercasing and stripping whitespace)
    scraped_df['NormalizedRestaurant'] = scraped_df['Restaurant'].str.lower().str.strip()
    scraped_df['NormalizedName'] = scraped_df['Name'].str.lower().str.strip()
    existing_df['NormalizedRestaurant'] = existing_df['Restaurant'].str.lower().str.strip()
    existing_df['NormalizedName'] = existing_df['Name'].str.lower().str.strip()
    
    # Ensure 'ScrapedDateTime' is in datetime format for proper comparison
    scraped_df['ScrapedDateTime'] = pd.to_datetime(scraped_df['ScrapedDateTime'])
    existing_df['ScrapedDateTime'] = pd.to_datetime(existing_df['ScrapedDateTime'])
    
    # Merge scraped data with existing data for comparison, using indicators to identify new items
    merged = pd.merge(scraped_df, existing_df, on=['NormalizedRestaurant', 'NormalizedName'], 
                      how='left', suffixes=('', '_existing'), indicator=True)
    
    # Identify new items or items with a price change using conditions based on merge indicators and price differences
    conditions = (
        # New items (not found in existing data)
        (merged['_merge'] == 'left_only') |
        # Items with a price change
        ((merged['DiscountedPrice'] != merged['DiscountedPrice_existing']) & pd.notnull(merged['DiscountedPrice_existing']))
    )
    updated_items = merged[conditions]

    # Prepare the DataFrame to include only new items or items with price changes, dropping unnecessary columns
    updated_items_cleaned = updated_items[['UUID', 'Restaurant', 'Name', 'Description', 'DiscountedPrice', 'ScrapedDateTime', 'url']]
    
    # Append these items to the existing dataset without removing or altering existing data
    final_df = pd.concat([existing_df, updated_items_cleaned], ignore_index=True).drop_duplicates()
    
    # Drop the extra columns added for normalization and comparison
    final_df.drop(columns=['NormalizedRestaurant', 'NormalizedName'], inplace=True)
    
    # Return the updated dataset with added new or updated items, maintaining historical integrity
    return final_df
