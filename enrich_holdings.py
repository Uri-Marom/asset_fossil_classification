# enrich_holdings.py
import pandas as pd


# Auxiliary functions
def id_col_clean(col):
    new_col = pd.Series(col.astype(str).str.strip().str.upper())
# remove '0' and other non valid values
    new_col = new_col.apply(
        lambda x: None if (x == '0') | (x == 'NAN') | (x == 'NONE') | (x == '') else x
    )
    return new_col


def any_heb_char(s):
    s = str(s)
    # df["has_hebrew_char"] = df[string_column].map(lambda s: any_heb_char(s))
    return any("\u0590" <= c <= "\u05EA" for c in s)


def id_col_types():
    """
    returns all available id column types
    :return: list
    """
    return ['ISIN', 'מספר תאגיד', 'LEI']


def id_col_patterns(id_type):
    """Patterns of all ID types: ISIN, il_corp_num and LEI

    :param id_type: string that is one of the following: "ISIN", "il_corp_num", "LEI"
    :return: the pattern of the input ID type
    """
    isin_pattern = r"^[A-Z]{2}([A-Z0-9]){9}[0-9]$"
    il_corp_num_pattern = r"^5([0-9]){8}$"
    lei_pattern = r"^[a-zA-Z0-9]{18}[0-9]{2}"
    patterns = {"ISIN": isin_pattern,
                "מספר תאגיד": il_corp_num_pattern,
                "LEI": lei_pattern}
    return patterns[id_type]


def find_id_col(df, id_type):
    """
    Automatically identify columns with the chosed id_type
    :param df: DataFrame
    :param id_type: str, one of the following: ISIN, LEI, il_corp_num
    :return: id_col: string
    """
    if id_type not in id_col_types():
        print("ERROR: {} is an unknown ID type".format(id_type))
        return
    pattern = id_col_patterns(id_type)
    max_cnt = 0
    for col in df:
        cnt = sum(df[col].astype(str).str.strip().str.contains(pattern, na=False))
        if cnt > max_cnt:
            id_col = col
            max_cnt = cnt

    if max_cnt > 0:
        # print("\nHolding file {} col is: {}".format(id_type, id_col))
        # print("number of {}s: {} out of {} rows".format(id_type, max_cnt, df.shape[0]))
        return id_col
    # else:
    #     print("\nno {}s in holdings file".format(id_type))


def find_id_cols(df):
    """get all ID columns for a given DataFrame

    :param df: DataFrame
    :return: a dictionary of {id_type:id_col} for all ID types
    """
    id_cols = {}
    for id_col_type in id_col_types():
        id_cols[id_col_type] = find_id_col(df, id_col_type)
    return id_cols


def fix_id_cols(df):
    id_cols = find_id_cols(df)
    # add columns if needed
    for id_type in id_col_types():
        if (id_type not in df.columns) & (id_cols[id_type] is not None):
            df[id_cols[id_type]] = id_col_clean(df[id_cols[id_type]])
            df[id_type] = df[id_cols[id_type]]
            # remove matching id_type from original column
            df[id_cols[id_type]] = df[id_cols[id_type]].str.replace(id_col_patterns(id_type), "")
            # remove non-matching id_type from new column
            matching_type = df[id_type].str.contains(id_col_patterns(id_type), na=False)
            df.loc[~matching_type, id_type] = ""
            # clean id cols
            df[id_cols[id_type]] = id_col_clean(df[id_cols[id_type]])
            df[id_type] = id_col_clean(df[id_type])
    return df
