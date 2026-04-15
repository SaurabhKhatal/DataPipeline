import pandas as pd


def process_blinkit(file_path):
    df = pd.read_csv(file_path)

    # basic column mapping
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["sku"] = df["item_id"]
    df["units"] = df["qty_sold"]
    df["revenue"] = df["mrp"]

    df["data_source"] = "blinkit"

    return df[["date", "sku", "units", "revenue", "data_source"]]


def process_zepto(file_path):
    df = pd.read_csv(file_path)

    # date format is different here (dd-mm-yyyy)
    df["date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y", errors="coerce").dt.date

    df["sku"] = df["SKU Number"]
    df["units"] = df["Sales (Qty) - Units"]
    df["revenue"] = df["Gross Merchandise Value"]

    df["data_source"] = "zepto"

    return df[["date", "sku", "units", "revenue", "data_source"]]


def process_nykaa(file_path):
    df = pd.read_csv(file_path)

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # nykaa already gives total qty and revenue
    df["sku"] = df["SKU Code"]
    df["units"] = df["Total Qty"]
    df["revenue"] = df["Selling Price"]

    df["data_source"] = "nykaa"

    return df[["date", "sku", "units", "revenue", "data_source"]]


def process_myntra(file_path):
    df = pd.read_csv(file_path)

    # date comes as YYYYMMDD
    df["date"] = pd.to_datetime(df["order_created_date"], format="%Y%m%d", errors="coerce").dt.date

    df["sku"] = df["style_id"]
    df["units"] = df["sales"]

    # need to subtract vendor discount to get actual revenue
    df["revenue"] = df["mrp_revenue"] - df["vendor_disc"]

    df["data_source"] = "myntra"

    return df[["date", "sku", "units", "revenue", "data_source"]]


def run_pipeline():
    # file paths (kept simple for now)
    blinkit_path = "C:/Users/Admin/Downloads/Assignment/Assignment/01-31-Jan-2026-Blinkit-Sales.csv"
    zepto_path = "C:/Users/Admin/Downloads/Assignment/Assignment/01-31-Jan-2026-Zepto.csv"
    nykaa_path = "C:/Users/Admin/Downloads/Assignment/Assignment/1-31-Jan-Nykaa-online.csv"
    myntra_path = "C:/Users/Admin/Downloads/Assignment/Assignment/myntra-jan26.csv"

    blinkit_df = process_blinkit(blinkit_path)
    zepto_df = process_zepto(zepto_path)
    nykaa_df = process_nykaa(nykaa_path)
    myntra_df = process_myntra(myntra_path)

    # combine all sources
    combined_df = pd.concat(
        [blinkit_df, zepto_df, nykaa_df, myntra_df],
        ignore_index=True
    )

    # basic cleaning
    combined_df = combined_df.dropna(subset=["date", "sku"])

    combined_df["units"] = combined_df["units"].fillna(0)
    combined_df["revenue"] = combined_df["revenue"].fillna(0)

    # aggregation
    final_df = (
        combined_df
        .groupby(["date", "sku", "data_source"], as_index=False)
        .agg({
            "units": "sum",
            "revenue": "sum"
        })
        .rename(columns={
            "units": "total_units",
            "revenue": "total_revenue"
        })
    )

    output_path = "C:/Users/Admin/Documents/daily_sales.csv"
    final_df.to_csv(output_path, index=False)

    print("Pipeline ran successfully")
    print("Output saved at:", output_path)


if __name__ == "__main__":
    run_pipeline()
