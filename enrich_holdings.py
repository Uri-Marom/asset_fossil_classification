# enrich_holdings.py

def id_col_types():
    '''
    returns all available id column types
    :return: list
    '''
    return ['ISIN', 'il_corp_num', 'LEI']


def id_col_patterns(id_type):
    isin_pattern = r"^[A-Z]{2}([A-Z0-9]){9}[0-9]$"
    il_corp_num_pattern = r"^5([0-9]){8}$"
    lei_pattern = r"^[a-zA-Z0-9]{18}[0-9]{2}"
    patterns = {"ISIN": isin_pattern,
                       "il_corp_num": il_corp_num_pattern,
                       "LEI": lei_pattern}
    return patterns[id_type]


def find_id_col(df, id_type):
    '''
    Automatically identify columns with the chosed id_type
    :param df: DataFrame
    :param id_type: str, one of the following: ISIN, LEI, il_corp_num
    :return: id_col: string
    '''
    if not id_type in id_col_types():
        print("ERROR: {} is an unknown ID type".format(id_type))
        return
    pattern = id_col_patterns(id_type)
    max_cnt = 0
    for col in df:
        cnt = sum(df[col].astype(str).str.contains(pattern, na=False))
        if cnt > max_cnt:
            id_col = col
            max_cnt = cnt

    if max_cnt > 0:
        print("\nHolding file {} col is: {}".format(id_type, id_col))
        print("number of {}s: {} out of {} rows".format(id_type, max_cnt, df.shape[0]))
        return id_col
    else:
        print("\nno {}s in holdings file".format(id_type))


def find_id_cols(df):
    id_cols = {}
    for id_col_type in id_col_types():
        id_cols[id_col_type] = find_id_col(df, id_col_type)
    return id_cols

# def