import pandas as pd


# =========================
# BLINKIT
# =========================
def process_blinkit(path):
    df = pd.read_csv(path)

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["sku"] = df["item_id"]
    df["units"] = df["qty_sold"]
    df["revenue"] = df["mrp"]

    df["data_source"] = "blinkit"

    return df[["date", "sku", "units", "revenue", "data_source"]]


# =========================
# ZEPTO
# =========================
def process_zepto(path):
    df = pd.read_csv(path)

    df["date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y", errors="coerce").dt.date
    df["sku"] = df["SKU Number"]
    df["units"] = df["Sales (Qty) - Units"]
    df["revenue"] = df["Gross Merchandise Value"]

    df["data_source"] = "zepto"

    return df[["date", "sku", "units", "revenue", "data_source"]]


# =========================
# NYKAA
# =========================
def process_nykaa(path):
    df = pd.read_csv(path)

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["sku"] = df["SKU Code"]
    df["units"] = df["Total Qty"]
    df["revenue"] = df["Selling Price"]

    df["data_source"] = "nykaa"

    return df[["date", "sku", "units", "revenue", "data_source"]]


# =========================
# MYNTRA
# =========================
def process_myntra(path):
    df = pd.read_csv(path)

    df["date"] = pd.to_datetime(df["order_created_date"], format="%Y%m%d", errors="coerce").dt.date
    df["sku"] = df["style_id"]
    df["units"] = df["sales"]

    # revenue = MRP - discount
    df["revenue"] = df["mrp_revenue"] - df["vendor_disc"]

    df["data_source"] = "myntra"

    return df[["date", "sku", "units", "revenue", "data_source"]]


# =========================
# MAIN PIPELINE
# =========================
def run_pipeline():
    # 👉 Update paths if needed
    blinkit = process_blinkit("C:/Users/Admin/Downloads/Assignment/Assignment/01-31-Jan-2026-Blinkit-Sales.csv")
    zepto = process_zepto("C:/Users/Admin/Downloads/Assignment/Assignment/01-31-Jan-2026-Zepto.csv")
    nykaa = process_nykaa("C:/Users/Admin/Downloads/Assignment/Assignment/1-31-Jan-Nykaa-online.csv")
    myntra = process_myntra("C:/Users/Admin/Downloads/Assignment/Assignment/myntra-jan26.csv")

    # Combine all sources
    df = pd.concat([blinkit, zepto, nykaa, myntra], ignore_index=True)

    # =========================
    # CLEANING
    # =========================
    df = df.dropna(subset=["date", "sku"])
    df["units"] = df["units"].fillna(0)
    df["revenue"] = df["revenue"].fillna(0)

    # =========================
    # AGGREGATION
    # =========================
    final_df = df.groupby(
        ["date", "sku", "data_source"],
        as_index=False
    ).agg(
        total_units=("units", "sum"),
        total_revenue=("revenue", "sum")
    )

    # =========================
    # OUTPUT
    # =========================
    output_path = "C:/Users/Admin/Documents/daily_sales.csv"
    final_df.to_csv(output_path, index=False)

    print("✅ Pipeline executed successfully!")
    print(f"📁 Output saved at: {output_path}")


if __name__ == "__main__":
    run_pipeline()