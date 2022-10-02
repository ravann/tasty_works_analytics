import pandas
import pandas as pd


class TradeDataPrep:
    """
        Class prepares trades from CSV transaction extract from TastyWorks activity website.
    """
    def __init__(self, hist_df: pandas.DataFrame) -> None:
        self.hist_df = hist_df.copy()
        self._fix_col_names()
        self._convert_dates()
        self.hist_df = self.hist_df.sort_values(by=["date_time"])
        self._convert_floats()
        self._fix_missing_data()
        self._set_open_close()
        self._set_buy_sell()

    def _convert_dates(self) -> None:
        self.hist_df["expiry_date"] = pd.to_datetime(self.hist_df.expiration_date, format='%m/%d/%y', errors='coerce')
        self.hist_df["date_time"] = pd.to_datetime(self.hist_df.date, format='%Y-%m-%dT%H:%M:%S%z', errors='coerce')
        # self.hist_df = self.hist_df.drop(columns=["expiration_date"])
        # self.hist_df = self.hist_df.rename(columns={"exp_date" : "expiration_date"})

    def _fix_col_names(self) -> None:

        col_map = {
            "action" : "trade_action", 
            "order_#" : "order_no"
        }

        cols = list(self.hist_df.columns)
        fcols: list = []
        for col in cols:
            col = col.lower()
            col = col.replace(" ", "_")
            col = col.replace("/", "_")

            if(col in col_map): 
                col = col_map[col]

            fcols.append(col)

        self.hist_df.columns = fcols


    def _convert_floats(self): 
        self.hist_df["commission"] = self.hist_df.apply(lambda x: float(x.commissions.replace("--", "0")), axis=1 )
        self.hist_df["value"] = self.hist_df.apply(lambda x: float(x.value.replace(",", "")), axis=1 )

    def _fix_missing_data(self) -> None: 
        # symbol
        self.hist_df["root_symbol"] = self.hist_df.apply(lambda x: x.root_symbol if (pd.notnull(x.root_symbol)) else x.symbol, axis = 1 )
        self.hist_df["underlying_symbol"] = self.hist_df.apply(lambda x: x.underlying_symbol if (pd.notnull(x.underlying_symbol)) else x.symbol, axis = 1 )

        # expiration_date, strike_price, call_or_put
        self.hist_df["expiration_date"] = self.hist_df.apply(lambda x: "12/31/99" if (x.instrument_type == "Equity") else x.expiration_date, axis = 1 )
        self.hist_df["strike_price"] = self.hist_df.apply(lambda x: "0" if (x.instrument_type == "Equity") else x.strike_price, axis = 1 )
        self.hist_df["call_or_put"] = self.hist_df.apply(lambda x: "NA" if (x.instrument_type == "Equity") else x.call_or_put, axis = 1 )

    def _set_open_close(self):
        self.hist_df["open_close"] = self.hist_df.apply(lambda x: "Open" if ( x.trade_action in ["BUY_TO_OPEN", "SELL_TO_OPEN"] ) else "Close", axis = 1 )

    def _set_buy_sell(self):
        self.hist_df["buy_sell"] = self.hist_df.apply(lambda x: "Buy" if ( x.trade_action in ["BUY_TO_OPEN", "BUY_TO_CLOSE"] ) else "Sell", axis = 1 )

    def get_prepared_data(self) -> pd.DataFrame: 
        return self.hist_df.copy()

    ### PREPARE TXN DATASET ###

    def get_txn_df(self) -> pandas.DataFrame:
        self._filter_trades()
        return self.txn_df

    def _filter_trades(self) -> None:
        self.txn_df: pandas.DataFrame = self.hist_df[self.hist_df.type.isin(['Trade', 'Receive Deliver'])]

