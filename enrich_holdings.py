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
    Automatically identify columns with the chosen id_type
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
        print("\nHolding file {} col is: {}".format(id_type, id_col))
        print("number of {}s: {} out of {} rows".format(id_type, max_cnt, df.shape[0]))
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


def prepare_mapping(mapping, by_id_type, add_id_type):
    """Prepare mapping DataFrame to be used for adding add_id_type by by_id_type to another DataFrame

    :param mapping: DataFrame having by_id_type and add_id_type
    :param by_id_type: the id_type to be used as key
    :param add_id_type: the id_type to be added
    :return: mapping ready to be used: by_id_type as unique index, add_id_type as only column
    """
    mapping[by_id_type] = id_col_clean(mapping[by_id_type])
    mapping[add_id_type] = id_col_clean(mapping[add_id_type])
    # remove rows with missing key or value
    mapping = mapping.dropna()
    mapping = mapping.drop_duplicates(by_id_type).set_index(by_id_type)
    mapping = mapping[add_id_type]
    return mapping


def add_id_by_another_id_mapping(df, add_id_type, by_id_type, mapping):
    """Add or update id_type by existing id_type and mapping

    :param df: DataFrame with by_id_type
    :param add_id_type: the id_type to be added
    :param by_id_type: the id_type by which to join
    :param mapping: a mapping between by_id_type (its index) and add_id_type (only column)
    :return: df with add_id_type (updated if existing already)
    """
    mapping = prepare_mapping(mapping, by_id_type, add_id_type)
    df[by_id_type] = id_col_clean(df[by_id_type])
    df_with_added_id_type = pd.merge(
        left=df,
        right=mapping,
        left_on=by_id_type,
        right_index=True,
        how='left',
        suffixes=['', '_new']
    )
    # if add_id_type already exist, update for missing values in original
    new_col = add_id_type + '_new'
    if new_col in df_with_added_id_type.columns:
        df_with_added_id_type[add_id_type] = df_with_added_id_type[add_id_type].fillna(
            df_with_added_id_type[new_col]
        )
        df_with_added_id_type.drop([new_col], axis=1, inplace=True)
    print("{}s with matching {}: {} out of total relevant rows: {}".format(
        by_id_type,
        add_id_type,
        df_with_added_id_type[add_id_type].notnull().sum(),
        df_with_added_id_type[by_id_type].notnull().sum()
    ))
    return df_with_added_id_type


def add_all_id_types_to_holdings(holdings, tlv_s2i, isin2lei):
    """Add all id_types to holdings DataFrame, using TLV mapping and ISIN2LEI

    :param holdings: holdings DataFrame, with מספר ני"ע and מספר מנפיק
    :param tlv_s2i: TLV securities mapping
    :param isin2lei: ISIN to LEI mapping
    :return: holdings Data with added id_types
    """
    holdings = fix_id_cols(holdings)
    holdings = add_id_by_another_id_mapping(holdings, add_id_type="ISIN", by_id_type='מספר ני"ע', mapping=tlv_s2i)
    holdings = add_id_by_another_id_mapping(holdings, add_id_type="מספר מנפיק", by_id_type="מספר תאגיד",
                                            mapping=tlv_s2i)
    holdings = add_id_by_another_id_mapping(holdings, add_id_type="מספר מנפיק", by_id_type='מספר ני"ע', mapping=tlv_s2i)
    holdings = add_id_by_another_id_mapping(holdings, add_id_type="מספר מנפיק", by_id_type='ISIN', mapping=tlv_s2i)
    holdings = add_id_by_another_id_mapping(holdings, add_id_type="LEI", by_id_type='ISIN', mapping=isin2lei)
    return holdings
