import pandas
import pandas as pd


class TradeMatcher:
    """
        Class matches trades from CSV transaction extract from TastyWorks activity website.
    """

    def __init__(self, txn_df: pandas.DataFrame) -> None:
        self.txn_df = txn_df

    def get_txn_df(self) -> pandas.DataFrame:
        return self.txn_df

    ### MATHCING LOGIC ###

    def _split_open_close(self) -> None:
        self.open_df: pandas.DataFrame = self.txn_df[self.txn_df.open_close == "Open"]
        self.close_df: pandas.DataFrame = self.txn_df[(self.txn_df.open_close == "Close")]

    def _match_trade(self, row: pandas.core.series.Series):
        """
        Match on symbol, expiration_date, strike, call_put
        If the source is Buy the match must be Sell
        :param row:
        :return: fees and amount
        """

        buy_sell = "Buy"
        if(row.buy_sell == "Buy"):
            buy_sell = "Sell"

        # Find records that will match
        matches_df = self.close_df[
            ( self.close_df.symbol == row.symbol ) &
            ( self.close_df.instrument_type == row.instrument_type ) &
            ( self.close_df.expiration_date == row.expiration_date ) &
            ( self.close_df.strike_price == row.strike_price ) &
            ( self.close_df.call_or_put == row.call_or_put ) &
            ( ( self.close_df.buy_sell == buy_sell ) | (self.close_df.type == 'Receive Deliver' ) ) &
            ( self.close_df.quantity > 0 )
        ]
        # print("Matched record index : ")
        idxs = matches_df.index.values.tolist()
        matched_rows = []
        left_quantity = row.quantity
        for idx in idxs:
            # Match until all the quantity from the open leg is matched
            if(left_quantity <= 0): 
                break
            close_quantity = self.close_df.loc[idx, 'quantity']
            if(close_quantity < left_quantity): 
                left_quantity = left_quantity - close_quantity
                self.close_df.loc[idx, 'quantity'] = 0
            else: 
                self.close_df.loc[idx, 'quantity'] = self.close_df.loc[idx, 'quantity'] - left_quantity
                left_quantity = 0
            comm = self.close_df.loc[idx, "commission"]
            fees = self.close_df.loc[idx, "fees"]
            amount = self.close_df.loc[idx, "value"]
            action = self.close_df.loc[idx, "action"] if len(str(self.close_df.loc[idx, "action"]).strip()) > 0 else self.close_df.loc[idx, "type"]
            close_dt = self.close_df.loc[idx, "date_time"]
            desc = self.close_df.loc[idx, "description"]
            t = ( action, close_dt, amount, comm, fees, close_quantity, desc )
            matched_rows.append(t)
        # print("No records match ... ")
        if(len(matched_rows) == 0): 
            return [(None, None, None, None, None, None, None)]
        else: 
            return matched_rows


    def match(self) -> pandas.DataFrame:
        self._split_open_close()
        self.open_df["output"] = self.open_df.apply(self._match_trade, axis=1)
        # print(len(self.open_df))
        # print(len(self.close_df))
        self.matched_df = self.open_df.explode("output")
        print(self.matched_df.head(2).T)
        self.matched_df[["close_action", "close_date_time", "close_value", "close_commission", "close_fees", "close_quantity", "close_description"]] = pd.DataFrame(self.matched_df['output'].tolist(), index=self.matched_df.index)
        self.matched_df = self.matched_df.drop(columns=["output"])
        return self.matched_df
