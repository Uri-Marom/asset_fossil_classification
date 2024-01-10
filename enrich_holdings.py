# enrich_holdings.py
import pandas as pd
import re


# Auxiliary functions
def id_col_clean(col):
    new_col = pd.Series(col.astype(str).str.strip().str.upper())
# remove '0' and other non valid values
    new_col = new_col.apply(
        lambda x: None if (x == '0') | (x == 'NAN') | (x == 'NONE') | (x == '') else x
    )
    return new_col


def remove_words_without_letters(s):
    l = s.split()
    words = [w for w in l if re.search('[a-zA-Zא-ת]', w)]
    return ' '.join(words)


def clean_company(s):
    s = str(s).upper()
    # remove special characters (dot, slash, asterisk ,percentage)
    s = re.sub(r'[\\/\\.\\%\\*\\"]+', '', s)
    # handle strings with "-", inc, ltd etc. - if long enough remove everything afterwards
    cut_from_list = ["-", " INC", " LTD", " CORP", " בעמ", " אגח", " PERP", "ORD"]
    cut_loc = min([s.find(c) for c in cut_from_list if s.find(c) > 0], default=-1)
    if cut_loc > 3:
        s = s[:cut_loc]
    # remove everything starting with a word that repeats
    l = s.split()
    rep_words = [w for w in l if s.count(w) > 1]
    if rep_words:
        first_rep_word = rep_words[0]
        pos = s.find(first_rep_word, s.find(first_rep_word) + 1)
        s = s[:pos]
    # remove non-letter characters
    pattern = r"[^א-תA-Za-z ]"
    s = re.sub(pattern, '', s)
    return s.strip()


def any_heb_char(s):
    s = str(s)
    # df["has_hebrew_char"] = df[string_column].map(lambda s: any_heb_char(s))
    return any("\u0590" <= c <= "\u05EA" for c in s)


def is_number(s):
    """checks if a string is a number

    :param s: string
    :return: true if s is a float, false otherwise
    """
    from math import isnan
    try:
        float(s)
        if not isnan(float(s)):
            return True
        else:
            return False
    except ValueError:
        return False


def is_il_holding(row):
    """a row level function, true if row is an Israeli holding based on security number and security name

    :param row: holding row
    :return: True if holding is Israeli holding, False else
    """
    sec_name = str(row["שם המנפיק/שם נייר ערך"]).upper().strip()
    sec_num = str(row['מספר ני"ע']).upper().strip()
    heb_char = any_heb_char(sec_name)
    numeric_sec_num = is_number(sec_num)
    sec_num_il_isin = sec_num.startswith("IL")
    if ("ISIN" in row) and (row["ISIN"] is not None):
        sec_ISIN = str(row["ISIN"]).upper().strip()
        sec_ISIN_il = sec_ISIN.startswith("IL")
        sec_num_il_isin = sec_num_il_isin | sec_ISIN_il
    return heb_char | numeric_sec_num | sec_num_il_isin


def bogus_issuer_numbers():
    """ a list of bogus issuer numbers, to be removed from any holding file

    :return: a list of bogus issuer numbers
    """
    bogus_issuer_nums = ["993", "994", "995"]
    return bogus_issuer_nums


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
    """Automatically identify columns with the chosen id_type

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
        # disregard ParentCorpId column
        if col not in ["ParentCorpLegalId"] and (col.find("parent_corp") == -1):
            cnt = sum(df[col].astype(str).str.strip().str.contains(pattern, na=False))
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
    """get all ID columns for a given DataFrame

    :param df: DataFrame
    :return: a dictionary of {id_type:id_col} for all ID types
    """
    id_cols = {}
    for id_col_type in id_col_types():
        id_cols[id_col_type] = find_id_col(df, id_col_type)
    return id_cols


def fix_id_cols(df):
    """Fix all ID columns for a DataFrame

    :param df: holdings DataFrame
    :return: holdings DataFrame with fixed ID columns
    """
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
    if "מספר מנפיק" in df.columns:
        # remove il_issuer_number for non Israeli holdings
        df_il_mask = df.apply(is_il_holding, axis='columns')
        df.loc[~df_il_mask, "מספר מנפיק"] = None
        # remove bogus issuer_number for all holdings
        df.loc[df["מספר מנפיק"].isin(bogus_issuer_numbers()), "מספר מנפיק"] = None
        df["מספר מנפיק"] = id_col_clean(df["מספר מנפיק"])
    return df


def ignore_id_types_holding_type():
    """Returns a dict of holding_type where id_type should be ignored per id_type

    :return: dict of id_type:[holding_types]
    """
    ignore_ids_at_holding_types = {
        'מספר ני"ע': ["הלוואות"],
        'ISIN': [],
        'מספר מנפיק': ["קרנות סל", "קרנות נאמנות"],
        'מספר תאגיד': ["קרנות סל", "קרנות נאמנות"],
        'LEI': ["קרנות סל", "קרנות נאמנות"],
    }
    return ignore_ids_at_holding_types


def get_non_fossil_holding_types():
    """get all non fossil holding types, e.g. cash holdings

    :return: a list of non fossil holdings types
    """
    non_fossil_holding_types = [
        'לא סחיר - תעודות התחייבות ממשלתי',
        'מזומנים',
        'פקדונות מעל 3 חודשים',
        'תעודות התחייבות ממשלתיות'
    ]
    return non_fossil_holding_types


def report_period_desc_to_date(period_desc):
    """translates report period desc (Hebrew text) to date

    :param period_desc: report period desc (Hebrew text)
    :return: report period as date
    """
    year = period_desc[0:4]
    quarter = period_desc[5:]
    if quarter == 'רבעון 1':
        quarter_date = '03-31'
    elif quarter == 'רבעון 2':
        quarter_date = '06-30'
    elif quarter == 'רבעון 3':
        quarter_date = '09-30'
    elif quarter == 'רבעון 4':
        quarter_date = '12-31'
    period_date = year + "-" + quarter_date
    return period_date


def fetch_latest_tlv_sec_num_to_issuer(path="data_sources/TASE mapping.csv"):
    # TODO: scrape from webpage
    #  "https://info.tase.co.il/_layouts/Tase/ManagementPages/Export.aspx?sn=none&GridId=106&AddCol=1&Lang=he-IL&CurGuid={6B3A2B75-39E1-4980-BE3E-43893A21DB05}&ExportType=3"
    df = pd.read_csv(path,
                     encoding="ISO-8859-8",
                     skiprows=3,
                     dtype=str
                     )
    # print("TLV sec num to issuer columns: {}".format(df.columns))
    return df


def fetch_latest_isin2lei(isin2lei_path="data_sources/ISIN_LEI.csv"):
    # TODO: Fetch automatically from website
    # https://www.gleif.org/en/lei-data/lei-mapping/download-isin-to-lei-relationship-files
    isin2lei = pd.read_csv(isin2lei_path)
    return isin2lei


def prepare_tlv_sec_num_to_issuer(tlv_s2i):
    tlv_s2i.columns = tlv_s2i.columns.str.strip()
    tlv_s2i["ISIN"] = id_col_clean(tlv_s2i["ISIN"])
    # handle Hebrew version
    if """מס' ני"ע""" in tlv_s2i.columns:
        tlv_s2i['מספר ני"ע'] = id_col_clean(tlv_s2i["""מס' ני"ע"""])
        tlv_s2i["מספר מנפיק"] = id_col_clean(tlv_s2i["מספר מנפיק"])
        tlv_s2i["מספר תאגיד"] = id_col_clean(tlv_s2i["מספר תאגיד"])
    elif "Security Number" in tlv_s2i.columns:
        tlv_s2i['מספר ני"ע'] = id_col_clean(tlv_s2i["Security Number"])
        tlv_s2i["מספר מנפיק"] = id_col_clean(tlv_s2i["Issuer No"])
        tlv_s2i["מספר תאגיד"] = id_col_clean(tlv_s2i["Corporate No"])
    return tlv_s2i


def prepare_mapping(mapping, by_id_type, add_id_type):
    """Prepare mapping DataFrame to be used for adding add_id_type by by_id_type to another DataFrame

    :param mapping: DataFrame having by_id_type and add_id_type
    :param by_id_type: the id_type to be used as key
    :param add_id_type: the id_type to be added
    :return: mapping ready to be used: by_id_type as unique index, add_id_type as only column
    """
    mapping[by_id_type] = id_col_clean(mapping[by_id_type])
    mapping[add_id_type] = id_col_clean(mapping[add_id_type])
    # fixed bug - removed rows with ANY NA before
    mapping = mapping[[by_id_type, add_id_type]]
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


def load_mappings_and_add_ids_to_holdings(holdings):
    """load needed id mappings and add ids to holdings DataFrame

    :param holdings: Dataframe
    :return: holdings with added id columns
    """
    tlv_s2i = prepare_tlv_sec_num_to_issuer(fetch_latest_tlv_sec_num_to_issuer())
    isin2lei = fetch_latest_isin2lei()
    # adding "I_" to ParentCorpLegalId to avoid confusion with il_corp_num
    holdings["ParentCorpLegalId"] = "I_" + holdings["ParentCorpLegalId"]
    # enrich holdings file - fix IDs
    holdings = add_all_id_types_to_holdings(holdings, tlv_s2i, isin2lei)
    return holdings


def add_fossil_sum(holdings):
    """ add fossil_sum to holdings DataFrame

    :param holdings: holdings DataFrame with is_fossil and sum columns
    :return: holdings with fossil_sum
    """
    holdings["שווי פוסילי"] = holdings["is_fossil"].astype(float) * holdings["שווי"].astype(float)
    return holdings
