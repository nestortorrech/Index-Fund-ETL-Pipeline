import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import time
import os
import requests

def fetch_and_store():

    # Step 1 Extract Data
    project = "Index Predictions"
    inputsFolder = os.path.join("C:/Career Projects/", project, "Inputs")
    outputsFolder = os.path.join("C:/Career Projects/", project, "Outputs")
    ticker_list = pd.read_csv(os.path.join(inputsFolder, "Index Funds.csv"), encoding='latin1')

    TIINGO_API_KEY = '1234567890'  # Anonymized Key
    headers = {'Content-Type': 'application/json'}

    tickers = ticker_list['Ticker'].tolist()
    all_data = []

   # Define date range for last 30 days
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')

    for i, ticker in enumerate(tickers):
        print(f"Fetching data for {ticker} ({i+1}/{len(tickers)})")

        url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
        params = {
        'startDate': start_date,
        'endDate': end_date,
        'token': TIINGO_API_KEY
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                print(f"Failed to fetch data for {ticker} - Status: {response.status_code}")
                continue

            data_json = response.json()
            if not data_json:
                print(f"No data for {ticker}")
                continue

            df = pd.DataFrame(data_json)
            df['Ticker'] = ticker
            df['date'] = pd.to_datetime(df['date'])
            df.rename(columns={
            'date': 'Date',
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'adjClose': 'Adj Close',
            'volume': 'Volume'
        }, inplace=True)

            all_data.append(df)

            time.sleep(1) 

        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
            continue

    # Combine all data
    if all_data:
        data = pd.concat(all_data, ignore_index=True)
        print(data.head())
        print(f"Total rows fetched: {len(data)}")
    else:
        print("No data fetched.")

   # data = pd.concat(all_data, ignore_index=True)

    # Step 2 Transform Data
    # Weekly Data

    # Make sure data is sorted properly
    long_data = data
    long_data = long_data.sort_values(['Ticker', 'Date'])

    # Daily Return as percentage change from previous day's close
    long_data['Daily_Return'] = long_data.groupby('Ticker')['Close'].pct_change() * 100  # multiplied by 100 for %
    long_data['Daily_Return']

    # 30-day Moving Average
    long_data['MA_7'] = long_data.groupby('Ticker')['Close'].transform(lambda x: x.rolling(window=7).mean())
    long_data['MA_7']

    # Volatility % during a single day. Not useful for Mutual Funds like SWPPX
    long_data['Volatility_PCT'] = ((long_data['Close'] - long_data['Open']) / long_data['Open']) * 100
    long_data['Volatility_PCT']

    # Add Month column for grouping later
    long_data['Month'] = pd.to_datetime(long_data['Date']).dt.strftime('%B %Y')
    long_data['Month']

    # Add Week Range
    start_of_week = long_data['Date'].dt.to_period('W-MON').apply(lambda r: r.start_time)
    end_of_week = start_of_week + pd.Timedelta(days=6)

    long_data['Week Range'] = start_of_week.dt.strftime('%Y-%m-%d') + ' to ' + end_of_week.dt.strftime('%Y-%m-%d')


    # Grouping by Month for Comparison

    long_data['Month'] = pd.to_datetime(long_data['Date']).dt.strftime('%B %Y')
    long_data['Month']

    # Grouping
    grouped_stats_weekly = long_data.groupby(['Month', 'Week Range', 'Ticker']).agg({
        'Volatility_PCT': 'mean',
        'Daily_Return': 'mean',
        'MA_7': 'mean'
    }).reset_index()


    grouped_stats_weekly.to_csv(os.path.join(outputsFolder, "grouped_indexes_weekly.csv"), index=False)

    # Step 3 SQL Upload
    engine = create_engine("postgresql://postgres:nestortorrech@localhost:5432/Portfolio")

    with engine.connect() as conn:
        # Load existing dates to avoid duplicates
        existing = pd.read_sql('SELECT "Ticker", "Week Range" FROM stock_prices_weekly_upd', conn)
        merged = pd.merge(grouped_stats_weekly, existing, on=["Ticker", "Week Range"], how="left", indicator=True)
        new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

        if not new_rows.empty:
            new_rows.to_sql("stock_prices_weekly_upd", conn, if_exists="append", index=False)
            print(f"Uploaded {len(new_rows)} new rows.")
        else:
            print("No new data to upload.")

# Run the pipeline
if __name__ == "__main__":
    fetch_and_store()

