# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import pandas as pd
import os
from TradeUtils.TradeMatcher import TradeMatcher
from TradeUtils.TradeDataPrep import TradeDataPrep

def fix_col_names(cols):
    fcols: list = []
    for col in cols:
        col = col.lower()
        col = col.replace(" ", "_")
        col = col.replace("/", "_")
        fcols.append(col)

    return fcols


def get_df_from_csv(filename):
    file = f"txns/{filename}"
    df = pd.read_csv(file)
    cols = list(df.columns)
    fcols = fix_col_names(cols)
    df.columns = fcols

    return df

def prep_match_df_for_reporting(df: pd.DataFrame): 
    df["open_date"] = df.apply(lambda x: x["date_time"].strftime("%Y-%m-%d"), axis = 1 )
    df["close_date"] = df.apply(lambda x: x["close_date_time"].strftime("%Y-%m-%d") if pd.notnull(x.close_date_time) else "", axis = 1 )

    df["value"] = df["value"] * df["close_quantity"] / df["quantity"]
    df["commission"] = df["commission"] * df["close_quantity"] / df["quantity"]
    df["fees"] = df["fees"] * df["close_quantity"] / df["quantity"]

    df["profit_loss"] = df.apply(lambda x: x["value"] + x["commission"] + x["fees"] + x["close_value"] + x["close_commission"] + x["close_fees"], axis = 1 )

    return df


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    files = os.listdir("txns/")
    latest_file = ""

    for file in files:
        if(not file.endswith(".csv")):
            continue;
        if (file > latest_file):
            latest_file = file

    print(f"Processing data from file : {latest_file}")
    pd.options.mode.chained_assignment = None

    # Prepare trade data
    df = get_df_from_csv(latest_file)
    tdp : TradeDataPrep = TradeDataPrep(df)
    txn_df = tdp.get_txn_df()

    # Match the trades
    tm : TradeMatcher = TradeMatcher(txn_df)
    match_df = tm.match()
    close_df = tm.close_df

    close_df.to_csv("close.csv", index=False)
    match_df = prep_match_df_for_reporting(match_df)

    print(match_df.dtypes)
    print(match_df.head(2).T)

    match_df.to_csv("output.csv", index=False)
