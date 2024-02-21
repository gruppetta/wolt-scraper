Web Scraping Project Overview
This project is designed to scrape data from a list of URLs, process this data to identify new or updated items, and update an existing dataset with this new information. It is divided into two main scripts:

1. settings_utils_functions.py: Contains settings, utility functions, and core scraping functions.
2. main_script.py: Orchestrates the scraping and data updating process.

Project Files
settings_utils_functions.py: Settings, utilities, and scraping functions.
main_script.py: The main script to run the project.
Restaurant_URL.csv: CSV file with URLs to scrape.
Rest_data_.csv: Existing dataset CSV file to be updated.

How It Works
The settings_utils_functions.py script sets up the necessary configurations and defines functions for URL fetching, parsing, and data processing.
The main_script.py script reads the URLs from Restaurant_URL.csv, performs the web scraping, and processes the data to find new or updated items.
It compares the scraped data against the existing dataset in Rest_data_.csv, appending any new or updated items.
The updated dataset is saved, preserving historical data and incorporating the latest information.

Getting Started
Ensure Python and the required packages (pandas, requests, beautifulsoup4, tqdm) are installed. Use the following command to install dependencies:

Copy code
pip install pandas requests beautifulsoup4 tqdm

To run the project:
Navigate to the project directory in a terminal.

Execute the script with:
python main_script.py


