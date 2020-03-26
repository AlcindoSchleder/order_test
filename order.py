#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os.path
import pandas as pd


class OrderData:

    df = None

    def __init__(self, filename: str = 'source_file_2.json'):
        if os.path.isfile(filename):
            self.df = pd.read_json(filename)

    @staticmethod
    def serialize_data_frame(df, field: str = 'managers'):
        result = []
        pk = ''
        line = {}
        for row in df.iterrows():
            if pk != row[1][field]:
                if len(line) > 0:
                    result.append(line)
                line = {row[1][field]: []}
            line[row[1][field]].append({
                'name': row[1]['name'],
                'priority': row[1]['priority']
            })
            pk = row[1][field]
        return result

    def sort_and_save(self, col: str):
        def classify_data_frame(column, idx):
            return (self.df[column].apply(
                lambda x: pd.Series(x[idx]) if idx == 0 or len(x) > 1 else pd.Series('')
            )
                    .stack()
                    .reset_index(level=1, drop=True)
                    .to_frame(column)
                    .join(self.df[['name', 'priority']], how='left')
                    )

        if self.df is not None and col in self.df.columns:
            xdf = classify_data_frame(col, 0)
            tdf = classify_data_frame(col, 1)

            tdf = tdf[tdf[col] != '']
            rdf = (pd.concat([xdf, tdf], ignore_index=True)
                   .groupby([col, 'priority', 'name'])
                   .first())
            rdf = rdf.reset_index()
            with open(f'{col}.json', 'w') as fp:
                json.dump(self.serialize_data_frame(rdf, col), fp)


order = OrderData()
order.sort_and_save('managers')
order.sort_and_save('watchers')
