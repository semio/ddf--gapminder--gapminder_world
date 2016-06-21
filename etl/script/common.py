# -*- coding: utf-8 -*-


def to_dict_dropna(data):
    """return a dictionary that do not contain any NaN values from a dataframe."""
    return dict((k, v.dropna().to_dict()) for k, v in data.iterrows())
