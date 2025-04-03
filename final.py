import time
import csv
import pandas as pd
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium import webdriver
import json
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service


def parse_float(x):
    """Convert a string like '1,234.56' into a float (1234.56)."""
    try:
        return float(x.replace(",", ""))
    except:
        return None


# -----------------------------
# 1) Set up Selenium WebDriver
# -----------------------------
GECKO_DRIVER_PATH = "/snap/bin/firefox.geckodriver"  # Update this path as needed

options = Options()
options.headless = True  # Run in headless mode
driver = webdriver.Firefox(service=Service(GECKO_DRIVER_PATH), options=options)

# URL for live market data for Nifty 50
url = "https://www.nseindia.com/market-data/live-equity-market?symbol=NIFTY%2050"

# -----------------------------
# 2) Continuous Scraping Loop (every 30 seconds)
# -----------------------------
while True:
    try:
        # Fetch the page and allow time for dynamic content to load
        driver.get(url)
        time.sleep(30)  # Adjust sleep if needed

        html_data = driver.page_source
        soup = BeautifulSoup(html_data, "html.parser")
        table = soup.find("table", {"id": "equityStockTable"})

        if not table:
            print("Error: Table not found. Check if the page loaded correctly.")
            time.sleep(30)
            continue

        # Parse table rows
        rows = table.find_all("tr")[1:]  # Skip header row
        data = []
        for row in rows:
            cols = row.find_all("td")
            # Skip rows that don't have enough columns
            if len(cols) < 15:
                continue

            symbol = cols[0].get_text(strip=True)
            open_price = parse_float(cols[1].get_text(strip=True))
            high_price = parse_float(cols[2].get_text(strip=True))
            low_price = parse_float(cols[3].get_text(strip=True))
            prev_close = parse_float(cols[4].get_text(strip=True))
            ltp = parse_float(cols[5].get_text(strip=True))
            change_val = parse_float(cols[7].get_text(strip=True))
            pct_chg = parse_float(cols[8].get_text(strip=True))
            volume_shares = parse_float(cols[9].get_text(strip=True))
            value_cr = parse_float(cols[10].get_text(strip=True))
            high_52w = parse_float(cols[11].get_text(strip=True))
            low_52w = parse_float(cols[12].get_text(strip=True))
            pct_chg_30d = parse_float(cols[13].get_text(strip=True))

            data.append(
                [
                    symbol,
                    open_price,
                    high_price,
                    low_price,
                    prev_close,
                    ltp,
                    change_val,
                    pct_chg,
                    volume_shares,
                    value_cr,
                    high_52w,
                    low_52w,
                    pct_chg_30d,
                ]
            )

        # Create DataFrame
        df = pd.DataFrame(
            data,
            columns=[
                "Symbol",
                "Open",
                "High",
                "Low",
                "PrevClose",
                "LTP",
                "Change",
                "%Chng",
                "Volume(shares)",
                "Value(Cr)",
                "52W_H",
                "52W_L",
                "30d_%Chng",
            ],
        )
        df = df.dropna(subset=["LTP", "%Chng"]).reset_index(drop=True)

        # -----------------------------------------
        # Compute Various Metrics
        # -----------------------------------------
        df_sorted = df.sort_values(by="%Chng", ascending=False)
        top5_gainers = df_sorted.head(5)
        top5_losers = df_sorted.tail(5).sort_values(by="%Chng")

        df_52w_below = df.dropna(subset=["52W_H"]).copy()
        df_52w_below = df_52w_below[df_52w_below["LTP"] <= 0.70 * df_52w_below["52W_H"]]
        below_52w_top5 = df_52w_below.head(5)

        df_52w_above = df.dropna(subset=["52W_L"]).copy()
        df_52w_above = df_52w_above[df_52w_above["LTP"] >= 1.20 * df_52w_above["52W_L"]]
        above_52w_top5 = df_52w_above.head(5)

        df_30d = df.dropna(subset=["30d_%Chng"]).copy()
        df_30d_sorted = df_30d.sort_values(by="30d_%Chng", ascending=False)
        top_30d_5 = df_30d_sorted.head(5)

        # ---------------------------
        # Save Full Data to CSV
        # ---------------------------
        df.to_csv("nifty50_data.csv", index=False)

        # ---------------------------
        # Create JSON with all requested fields (comprehensive results)
        # ---------------------------
        results = {
            "top_5_gainers": top5_gainers.to_dict(orient="records"),
            "top_5_losers": top5_losers.to_dict(orient="records"),
            "stocks_30_percent_below_52W_high": below_52w_top5.to_dict(
                orient="records"
            ),
            "stocks_20_percent_above_52W_low": above_52w_top5.to_dict(orient="records"),
            "top_5_30_day_returns": top_30d_5.to_dict(orient="records"),
        }
        with open("nifty50_results.json", "w") as json_file:
            json.dump(results, json_file, indent=4)

        # ---------------------------
        # Create a Separate JSON Summary for Console Output
        # ---------------------------
        summary = {
            "data_update_time": time.ctime(),
            "top_5_gainers": top5_gainers[["Symbol", "%Chng", "LTP"]].to_dict(
                orient="records"
            ),
            "top_5_losers": top5_losers[["Symbol", "%Chng", "LTP"]].to_dict(
                orient="records"
            ),
            "stocks_30_percent_below_52W_high": below_52w_top5[
                ["Symbol", "LTP", "52W_H"]
            ].to_dict(orient="records"),
            "stocks_20_percent_above_52W_low": above_52w_top5[
                ["Symbol", "LTP", "52W_L"]
            ].to_dict(orient="records"),
            "top_5_30_day_returns": top_30d_5[["Symbol", "30d_%Chng", "LTP"]].to_dict(
                orient="records"
            ),
        }
        with open("nifty50_summary.json", "w") as summary_file:
            json.dump(summary, summary_file, indent=4)

        # ---------------------------
        # Create and Save Bar Chart for Top 5 Gainers/Losers
        # ---------------------------
        plt.figure(figsize=(12, 5))
        plt.subplot(1, 2, 1)
        plt.bar(top5_gainers["Symbol"], top5_gainers["%Chng"], color="green")
        plt.title("Top 5 Gainers (Today)")
        plt.xlabel("Symbol")
        plt.ylabel("% Change")
        plt.xticks(rotation=45)

        plt.subplot(1, 2, 2)
        plt.bar(top5_losers["Symbol"], top5_losers["%Chng"], color="red")
        plt.title("Top 5 Losers (Today)")
        plt.xlabel("Symbol")
        plt.ylabel("% Change")
        plt.xticks(rotation=45)

        plt.tight_layout()
        plt.savefig("top5_gainers_losers.png")
        plt.close()

        # ---------------------------
        # Print Summary Results to Console
        # ---------------------------
        print("\n===== Data Update at", time.ctime(), "=====")
        print("\n----- TOP 5 GAINERS -----")
        print(top5_gainers[["Symbol", "%Chng", "LTP"]])
        print("\n----- TOP 5 LOSERS -----")
        print(top5_losers[["Symbol", "%Chng", "LTP"]])
        print("\n----- 5 Stocks >=30% Below 52W High -----")
        print(below_52w_top5[["Symbol", "LTP", "52W_H"]])
        print("\n----- 5 Stocks >=20% Above 52W Low -----")
        print(above_52w_top5[["Symbol", "LTP", "52W_L"]])
        print("\n----- Top 5 Stocks by 30d %Chng -----")
        print(top_30d_5[["Symbol", "30d_%Chng", "LTP"]])

    except Exception as e:
        print("An error occurred:", e)

    # Wait for 30 seconds before the next update
    time.sleep(30)
