import pandas
import pandas as pd


class TradeDataPrep:
    """
        Class prepares trades from CSV transaction extract from TastyWorks activity website.
    """
    def __init__(self, hist_df: pandas.DataFrame) -> None:
        self.hist_df = hist_df
        self._convert_dates()
        self.hist_df = hist_df.sort_values(by=["date_time"])
        self._convert_floats()

    def _convert_dates(self) -> None:
        self.hist_df["expiration_date"] = pd.to_datetime(self.hist_df.expiration_date, format='%m/%d/%y', errors='coerce')
        self.hist_df["date_time"] = pd.to_datetime(self.hist_df.date, format='%Y-%m-%dT%H:%M:%S%z', errors='coerce')

    def _convert_floats(self): 
        self.hist_df["commission"] = self.hist_df.apply(lambda x: float(x.commissions.replace("--", "0")), axis=1 )
        self.hist_df["value"] = self.hist_df.apply(lambda x: float(x.value.replace(",", "")), axis=1 )


    ### PREPARE TXN DATASET ###

    def get_txn_df(self) -> pandas.DataFrame:
        self._filter_trades()
        self._fix_missing_data()
        self._set_open_close()
        self._set_buy_sell()
        return self.txn_df

    def _filter_trades(self) -> None:
        self.txn_df: pandas.DataFrame = self.hist_df[self.hist_df.type.isin(['Trade', 'Receive Deliver'])]

    def _fix_missing_data(self) -> None: 
        # symbol
        self.txn_df["root_symbol"] = self.txn_df.apply(lambda x: x.root_symbol if (pd.notnull(x.root_symbol)) else x.symbol, axis = 1 )
        self.txn_df["underlying_symbol"] = self.txn_df.apply(lambda x: x.underlying_symbol if (pd.notnull(x.underlying_symbol)) else x.symbol, axis = 1 )

        # expiration_date, strike_price, call_or_put
        self.txn_df["expiration_date"] = self.txn_df.apply(lambda x: "12/31/99" if (x.instrument_type == "Equity") else x.expiration_date, axis = 1 )
        self.txn_df["strike_price"] = self.txn_df.apply(lambda x: "0" if (x.instrument_type == "Equity") else x.strike_price, axis = 1 )
        self.txn_df["call_or_put"] = self.txn_df.apply(lambda x: "NA" if (x.instrument_type == "Equity") else x.call_or_put, axis = 1 )


    def _set_open_close(self):
        self.txn_df["open_close"] = self.txn_df.apply(lambda x: "Open" if ( x.action in ["BUY_TO_OPEN", "SELL_TO_OPEN"] ) else "Close", axis = 1 )
        # print(self.txn_df[self.txn_df.type == "Receive Deliver"].head(2).T)

    def _set_buy_sell(self):
        self.txn_df["buy_sell"] = self.txn_df.apply(lambda x: "Buy" if ( x.action in ["BUY_TO_OPEN", "BUY_TO_CLOSE"] ) else "Sell", axis = 1 )
        self.txn_df.to_csv("buy_sell.csv", index=False)

