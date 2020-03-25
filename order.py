#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os.path
import pandas as pd


class OrderData:

    df = None

    def __init__(self, filename: str = 'source_file_2.json'):
        if os.path.isfile(filename):
            self.df = pd.read_json(filename)

    def sort_and_save(self, col: str):
        if self.df is not None and col in self.df.columns:
            tdf = (self.df[col].apply(lambda x: pd.Series(x[0]))
                   .stack()
                   .reset_index(level=1, drop=True)
                   .to_frame(col)
                   .join(self.df[['name', 'priority']], how='left')
            )
            tdf = tdf.sort_values([col, 'priority'], ascending=[True, False])
            tdf.to_json(f'./{col}.json', orient='records')


order = OrderData()
order.sort_and_save('managers')
order.sort_and_save('watchers')
