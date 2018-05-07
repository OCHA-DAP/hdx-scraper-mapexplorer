#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
helper functions
----------------

"""
import pandas as pd
from hdx.data.resource import Resource


class OutputError(Exception):
    pass


def drop_columns(df, columns_to_keep):
    df = df.loc[:, columns_to_keep]
    return df


def hxlate(df, hxl_names):
    hxl_columns = [hxl_names[c] for c in df.columns]
    hxl = pd.DataFrame.from_records([hxl_columns], columns=df.columns)
    df = pd.concat([hxl, df])
    df.reset_index(inplace=True, drop=True)
    return df


def cannonize_name(value, valid_names, replace_values):
    value = value.strip().title()
    value = replace_values.get(value, value)
    if value not in valid_names:
        raise OutputError('Invalid location (not in names list): %s' % value)
    return value