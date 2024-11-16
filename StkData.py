from requests_html import HTMLSession
import pandas as pd
import os

def get_stock_data(symbol, session):
    # Step 1: Navigate to NSE homepage to establish session
    homepage_url = "https://www.nseindia.com"
    session.get(homepage_url)
    
    # Step 2: Navigate to the stock quote page with the symbol
    url = f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}"
    session.get(url)
    
    # Step 3: Navigate to the API URL to fetch the stock data
    api_url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
    api_response = session.get(api_url)

    # Check if the API response is successful
    if api_response.status_code == 200:
        # Parse JSON data
        data = api_response.json()

        # Extract relevant industry information
        basic_industry = data.get('industryInfo', {}).get('basicIndustry', None)
        industry = data.get('industryInfo', {}).get('industry', None)
        macro = data.get('industryInfo', {}).get('macro', None)
        sector = data.get('industryInfo', {}).get('sector', None)

        # Return a tuple of extracted data
        return basic_industry, industry, macro, sector
    else:
        # Handle the case where the API call fails
        error_text = f"Request for symbol {symbol} failed with status code {api_response.status_code}"
        print(error_text)
        return None

# Step 1: Read symbols from "Symbols.csv" file
symbols_df = pd.read_csv('Symbols.csv')

# Print the first few rows to confirm successful read
print(symbols_df.head())

# Step 2: Create an HTML session
session = HTMLSession()

# Fields to track in the resulting DataFrame
columns = ['symbol', 'basic_industry', 'industry', 'macro', 'sector']

# Create an empty DataFrame to store the results
stkdata_df = pd.DataFrame(columns=columns)

# Step 3: Iterate through each symbol and fetch data
for index, row in symbols_df.iterrows():
    # Extract the symbol from the first column of each row
    symbol = row.iloc[0]

    # Fetch stock data for the symbol
    industry_data = get_stock_data(symbol, session)

    # If data is fetched successfully, append it to the DataFrame
    if industry_data is not None:
        basic_industry, industry, macro, sector = industry_data  # Unpack the tuple

        # Print the fetched data for logging/debugging purposes
        print(f"Stock data for {symbol}: {industry_data}")

        # Add a new row to the DataFrame with the fetched data
        stkdata_df = stkdata_df._append({'symbol': symbol, 'basic_industry': basic_industry, 'industry': industry, 'macro': macro, 'sector': sector}, ignore_index=True)
    else:
        print(f"Error fetching industry data for {symbol}")

# Step 4: Save the resulting DataFrame to a CSV file in the specified directory
output_directory = "/home/nikunj/NseInsiderTrading"
os.makedirs(output_directory, exist_ok=True)  # Ensure the directory exists
output_file = os.path.join(output_directory, "stkdata.csv")  # Full path for the output file

stkdata_df.to_csv(output_file, index=False)

print(f"Stock data saved to {output_file}")

# Step 5: Close the session at the end
session.close()
