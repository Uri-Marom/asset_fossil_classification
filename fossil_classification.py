# fossil_classification.py

import numpy as np
import pandas as pd
import re
import string
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from bs4 import BeautifulSoup
import urllib.request
pd.set_option('display.max_columns', None)


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


def clean_ticker(s):
    s = str(s)
    # remove trailing digits
    without_trailing_digits = s.rstrip(string.digits)
    # remove everything after a dot or a space from tickers.
    return re.sub(r'[\\/%\s]+$', '', without_trailing_digits).partition('.')[0].partition(' ')[0]


def clean_instrument_from_ticker(df, instrument_col, ticker_col):
    # remove ticker and everything that follows from instrument name if it appears in the 3rd word or later
    # e.g. AKER BP ASA AKERBP 4 3/4 06/15/24 --> AKER BP, AIB GROUP PLC AIB 5 1/4 PERP --> AIB GROUP PLC
    instruments = df
    instruments["instrument_word_list"] = instruments[instrument_col].str.split()
    instruments["ticker_first"] = instruments[ticker_col].str.split().str.get(0)
    instruments["ticker_in_name"] = instruments.apply(
        lambda row: row["instrument_word_list"][1:].index(row["ticker_first"]) + 1
        if row["ticker_first"] in row["instrument_word_list"][1:]
        else np.nan,
        axis=1)
    instruments["company_name_cut_ticker"] = instruments.apply(
        lambda row: ' '.join(row["instrument_word_list"][:int(row["ticker_in_name"])]) if row["ticker_in_name"] > 1
        else row[instrument_col],
        axis=1)
    return instruments.drop(["instrument_word_list", "ticker_first", "ticker_in_name"], axis=1)


def clean_company(s):
    s = str(s)
    return re.sub(r'[\\/.%\s\d]+$', '', s)


def company_names_match_score(row, holdings_company_col, fff_company_col, min_len=3):
    holdings_company_name = str(row[holdings_company_col]).strip().lower()
    fff_company_name = str(row[fff_company_col]).strip().lower()
    if (holdings_company_name == 'nan') | (fff_company_name == 'nan'):
        return np.nan
    if (len(holdings_company_name) >= min_len) & (len(fff_company_name) >= min_len):
        return fuzz.partial_ratio(holdings_company_name, fff_company_name)


def get_common_words_in_company_name(holdings, fff, holdings_company_col, fff_company_col):
    # returns a list of common words, to be disregarded when matching by company names
    # print(fff[fff_company_col].str.split(expand=True).stack().value_counts().head(30))
    # print(holdings[holdings_company_col].str.split(expand=True).stack().value_counts().head(30))
    common_words_company_name = ['LTD', 'INC', 'CORP', 'CO', 'GROUP', 'PLC', 'HOLDINGS', '&', 'FLOAT', 'אגח']
    return common_words_company_name


def remove_common_words(l, common):
    res = []
    for x in l:
        x = str(x)
        new = ' '.join([word for word in x.split() if word not in common])
        res.append(new)
    return res


def find_isin_col(df):
    '''
    Automatically identify columns with ISINs
    :param df: DataFrame
    :return: isin_col: string
    '''
    isin_pattern = r"^[A-Z]{2}([A-Z0-9]){9}[0-9]$"
    max_isin_cnt = 0
    for col in df:
        isin_cnt = sum(df[col].astype(str).str.contains(isin_pattern, na=False))
        if isin_cnt > max_isin_cnt:
            isin_col = col
            max_isin_cnt = isin_cnt

    if max_isin_cnt > 0:
        print("\nHolding file ISIN col is: " + isin_col)
        print("number of ISINs: {} out of {} rows".format(max_isin_cnt, df.shape[0]))
        return isin_col
    else:
        print("\nERROR: no ISINs in holdings file")


def find_il_corp_num_col(df):
    '''
    Automatically identify columns with Israeli Corp Numbers (מספר תאגיד)
    :param df:
    :return:
    '''
    pattern = r"^5([0-9]){8}$"
    max_pattern_cnt = 0
    for col in df:
        pattern_cnt = sum(df[col].astype(str).str.contains(pattern, na=False))
        if pattern_cnt > max_pattern_cnt:
            max_col = col
            max_pattern_cnt = pattern_cnt

    if max_pattern_cnt > 0:
        print("\nHolding file Israel Corp col is: " + max_col)
        print("number of Israel Corp Numbers: {} out of {} rows".format(max_pattern_cnt, df.shape[0]))
        return max_col
    else:
        print("\nERROR: no Israel Corp Numbers in holdings file")


def is_tlv(df, isin_col):
    return df[isin_col].str.isdigit().fillna(False)


# Fossil Free Funds list functions
def fetch_latest_fff_list():
    # fetch newest file from Fossil Free Funds - stopped working (asking for email), reverted to manual download
    # returns Dataframe read from excel file
    # site = "https://fossilfreefunds.org/how-it-works"
    # hdr = {
    #     'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
    #     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    #     'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
    #     'Accept-Encoding': 'none',
    #     'Accept-Language': 'en-US,en;q=0.8',
    #     'Connection': 'keep-alive'}
    # req = urllib.request.Request(site, headers=hdr)
    # html_page = urllib.request.urlopen(req)
    # soup = BeautifulSoup(html_page, "html.parser")
    # links_in_page = [link.get('href') for link in soup.findAll('a')]
    # fff_latest_company_screens_url = [l for l in links_in_page if 'Invest+Your+Values+company+screens' in l][0]
    # print("\n** Fetching latest Fossil Free Funds company screens list **")
    fff_latest_company_screens_url = "/Users/urimarom/Downloads/Invest+Your+Values+company+screens+20210909.xlsx"
    print("Using "+fff_latest_company_screens_url)
    return pd.read_excel(fff_latest_company_screens_url, sheet_name=1)


def prepare_fff(df, fossil_only=False):
    # Input: Fossil Free Funds list as dataframe
    # Output:
    # map fossil flags to 1/0 instead of Y/None
    fff_cols = [c for c in df.columns if 'Fossil Free' in c]
    df[fff_cols] = df[fff_cols].applymap(lambda x: 1 if x == 'Y' else 0)
    # define fossil criteria := any of these are true: coal, oil / gas or fossil-fired utility
    criteria = (df['Fossil Free Funds: Coal screen'] +
                df['Fossil Free Funds: Oil / gas screen'] +
                df['Fossil Free Funds: Fossil-fired utility screen']
                ) > 0
    df['fff_fossil_any'] = criteria.astype(int)
    print("\nis_fossil in Fossil Free Funds list")
    print(df['fff_fossil_any'].value_counts(dropna=False))
    print("\nFossil tags breakdown")
    print(
        pd.crosstab(
            df['Fossil Free Funds: Coal screen'],
            [
                df['Fossil Free Funds: Oil / gas screen'],
                df['Fossil Free Funds: Fossil-fired utility screen']
            ],
            rownames=["Coal"],
            colnames=["Oil / Gas", "Utilities"],
            dropna=False
        )
    )
    df['Company'] = df['Company'].str.upper().str.strip()
    df['Tickers'] = df['Tickers'].str.upper().str.strip()
    # narrow down to companies tagged as fossil only
    if fossil_only:
        fff = df[criteria]
    else:
        fff = df
    # explode lists, to get one row per ticker
    fff = fff.assign(Tickers=fff['Tickers'].str.split(',')).explode('Tickers')
    # remove irrelevant columns
    id_cols = ["Company", "Country", "Tickers"]
    fff = fff[id_cols + fff_cols + ['fff_fossil_any']]
    fff = fff[fff['Tickers'].notnull()]
    fff['Tickers'] = fff['Tickers'].str.strip().str.upper()
    return fff


# TLV (TASE) companies list, maintained by Clean Money Forum
# TODO: download file from a repository or db instead of using local
def fetch_latest_tlv_list(tlv_path):
    tlv = pd.read_excel(tlv_path, sheet_name=0, skiprows=range(3))
    print("\n** Fetching tlv companies fossil classification **")
    return tlv


def prepare_tlv(tlv):
    tlv.columns = tlv.columns.str.strip()
    tlv["מספר מנפיק"] = id_col_clean(tlv["מספר מנפיק"])
    tlv["מספר תאגיד"] = id_col_clean(tlv["מספר תאגיד"])

    def ken_lo_to_binary(s):
        if s.startswith('כן'):
            return 1
        elif s.startswith('לא'):
            return 0
        else:
            return np.nan

    tlv["רשימה שחורה"] = tlv["רשימה שחורה"].map(ken_lo_to_binary)
    print("\nis_fossil in TLV companies classification")
    print(tlv["רשימה שחורה"].value_counts(dropna=False))
    print("\n*** TLV companies with missing fossil classification ***")
    print(tlv[tlv["רשימה שחורה"].isnull()])
    return tlv


# TODO: download file from a repository or db instead of using local
def fetch_latest_prev_classified(prev_class_path):
    prev_fossil_classification = pd.read_csv(prev_class_path)
    return prev_fossil_classification


# 3. Previously classified, adding issuers and LEI
def prepare_prev_class(prev_class):
    prev_class["security_num"] = id_col_clean(prev_class["security_num"])
    prev_class["issuer_or_corp_num"] = id_col_clean(prev_class["issuer_or_corp_num"])
    print("\n** Previously classified ISINs and corps **")
    print("is_fossil in previously classified")
    print(prev_class["is_fossil"].value_counts(dropna=False))
    prev_grouped_by_sec_num = prev_class.groupby("security_num")
    sec_num_with_diff_class = prev_grouped_by_sec_num.filter(lambda x:
                                                             (0 < x["is_fossil"].mean() < 1)
                                                             )
# TODO: raise warning for ambiguously classified
    if len(sec_num_with_diff_class) > 0:
        print("\n*** Securities with both is_fossil=1 and is_fossil=0 ***")
        print(sec_num_with_diff_class)

    prev_grouped_by_issuer = prev_class[prev_class["issuer_or_corp_num"] != 'NAN'].groupby("issuer_or_corp_num")
    issuer_with_diff_class = prev_grouped_by_issuer.filter(lambda x:
                                                           (0 < x["is_fossil"].mean() < 1)
                                                           )
    if len(issuer_with_diff_class) > 0:
        print("\n*** Issuers with both is_fossil=1 and is_fossil=0 ***")
        print(issuer_with_diff_class)
    return prev_class


def prepare_holdings(holdings_path, sheet_num):
    if holdings_path.lower().endswith(".xls") | holdings_path.lower().endswith(".xlsx"):
        # TODO: handle multiple sheets - run one by one?
        holdings = pd.read_excel(holdings_path, sheet_name=sheet_num)
    elif holdings_path.lower().endswith(".csv"):
        holdings = pd.read_csv(holdings_path)
    else:
        # TODO: return error
        print("holdings input file isn't Excel or CSV file")
        return
    print("\n** Holdings file for classification **")
    print(holdings_path)
    holdings.columns = holdings.columns.str.strip()
    print("columns: {}".format(holdings.columns))
    isin_col = find_isin_col(holdings)
    holdings[isin_col] = id_col_clean(holdings[isin_col])
    il_corp_col = find_il_corp_num_col(holdings)
    if il_corp_col:
        holdings[il_corp_col] = id_col_clean(holdings[il_corp_col])
    return holdings, isin_col, il_corp_col


def fetch_latest_tlv_sec_num_to_issuer():
    # TODO: scrape from webpage
    #  "https://info.tase.co.il/_layouts/Tase/ManagementPages/Export.aspx?sn=none&GridId=106&AddCol=1&Lang=he-IL&CurGuid={6B3A2B75-39E1-4980-BE3E-43893A21DB05}&ExportType=3"
    df = pd.read_csv("/Users/urimarom/Downloads/Data_20210529.csv",
                     encoding="ISO-8859-8",
                     skiprows=3,
                     dtype={"מספר תאגיד": str}
                     )
    # print("TLV sec num to issuer columns: {}".format(df.columns))
    return df


def fetch_latest_isin2lei(isin2lei_path):
    # TODO: Fetch automatically from website
    # https://www.gleif.org/en/lei-data/lei-mapping/download-isin-to-lei-relationship-files
    isin2lei = pd.read_csv(isin2lei_path, index_col=1)
    return isin2lei


def prepare_tlv_sec_num_to_issuer(tlv_s2i):
    tlv_s2i.columns = tlv_s2i.columns.str.strip()
    tlv_s2i["""מס' ני"ע"""]= id_col_clean(tlv_s2i["""מס' ני"ע"""])
    tlv_s2i["ISIN"] = id_col_clean(tlv_s2i["ISIN"])
    tlv_s2i["מספר מנפיק"] = id_col_clean(tlv_s2i["מספר מנפיק"])
    tlv_s2i["מספר תאגיד"] = id_col_clean(tlv_s2i["מספר תאגיד"])
    return tlv_s2i


def choose_best_issuer_num(row):
    if not row["מספר מנפיק"]:
        return row["מספר מנפיק_x"]
    elif not row["מספר מנפיק_x"]:
        return row["מספר מנפיק"]
    else:
        if (len(str(row["מספר מנפיק"])) < len(str(row["מספר מנפיק_x"]))) & (len(str(row["מספר מנפיק"])) > 0):
            return row["מספר מנפיק"]
        else:
            return row["מספר מנפיק_x"]


def add_tlv_issuer_by_col(df, mapping, holdings_join_col, mapping_join_col):
    mapping = mapping.groupby(mapping_join_col).first()
    df_with_issuer = pd.merge(left=df,
                              right=mapping['מספר מנפיק'],
                              left_on=holdings_join_col,
                              right_index=True,
                              how='left'
                              )
    # TODO: use both original issuer number (if exists) and new one from mapping
    if "מספר מנפיק_y" in df_with_issuer.columns:
        # choose the more accurate issuer number
        df_with_issuer.rename({"מספר מנפיק_y":"מספר מנפיק"}, axis=1, inplace=True)
        df_with_issuer["מספר מנפיק"] = id_col_clean(df_with_issuer["מספר מנפיק"])
        df_with_issuer["מספר מנפיק"] = df_with_issuer.apply(choose_best_issuer_num, axis='columns')
        df_with_issuer = df_with_issuer.drop(['מספר מנפיק_x'], axis=1)
    df_with_issuer["מספר מנפיק"] = id_col_clean(df_with_issuer["מספר מנפיק"])
    print("Holdings with matching issuer number after joining by {}: {} out of total holdings {}".format(
        holdings_join_col,
        df_with_issuer["מספר מנפיק"].notnull().sum(),
        df_with_issuer.shape[0]
    ))
    return df_with_issuer


def add_tlv_issuer_by_ticker(
        df,
        mapping,
        df_isin_col,
        df_ticker_col,
        df_issuer_col,
        mapping_heb_ticker_col,
        mapping_eng_ticker_col
):
    # try exact match by ticker symbol, after removing trailing numbers
    # deal only with TLV securities without an issuer
    # handle hebrew and english separately
    df_tlv_mask = is_tlv(df, df_isin_col)
    df_tlv = df[df_tlv_mask]
    # remove trailing digits from tickers
    df_tlv["clean_ticker"] = df_tlv[df_ticker_col].map(lambda s: clean_ticker(s))
    # handle hebrew tickers
    df_tlv_heb_ticker = df_tlv[df_tlv[df_ticker_col].map(lambda s: any_heb_char(s))]
    # focus on tickers with no issuer
    df_tlv_heb_ticker_no_issuer = df_tlv_heb_ticker[df_tlv_heb_ticker[df_issuer_col].isnull()]
    mapping_heb = mapping[[mapping_heb_ticker_col, 'מספר מנפיק']]
    mapping_heb[mapping_heb_ticker_col] = mapping_heb[mapping_heb_ticker_col].map(lambda s: clean_ticker(s))
    mapping_heb = mapping_heb.groupby(mapping_heb_ticker_col).first()
    merge_by_heb_ticker = pd.merge(
        df_tlv_heb_ticker_no_issuer[[df_isin_col, "clean_ticker"]],
        mapping_heb,
        left_on="clean_ticker",
        right_on=mapping_heb_ticker_col,
        how='inner'
    )
    # do the same for English tickers
    mapping_eng = mapping[[mapping_eng_ticker_col, 'מספר מנפיק']]
    mapping_eng[mapping_eng_ticker_col] = mapping_eng[mapping_eng_ticker_col].map(lambda s: clean_ticker(s))
    mapping_eng = mapping_eng.groupby(mapping_eng_ticker_col).first()
    df_tlv_no_issuer = df_tlv[df_tlv[df_issuer_col].isnull()]
    merge_by_eng_ticker = pd.merge(
        df_tlv_no_issuer[[df_isin_col, "clean_ticker"]],
        mapping_eng,
        left_on="clean_ticker",
        right_on=mapping_eng_ticker_col,
        how='inner'
    )
    # put the results together
    isin2issuer_through_ticker = pd.concat([merge_by_heb_ticker, merge_by_eng_ticker])
    isin2issuer_through_ticker.rename({"מספר מנפיק": "issuer_by_ticker"}, axis=1, inplace=True)
    isin2issuer_through_ticker = isin2issuer_through_ticker.groupby(df_isin_col).first()
    df = pd.merge(
        df,
        isin2issuer_through_ticker["issuer_by_ticker"],
        left_on=df_isin_col,
        right_index=True,
        how='left'
    )
    # use issuer by ticker to fill na in issure column
    print("number of holdings with issuer before adding issuers by ticker: {}".format(
        df[df_issuer_col].notnull().sum()
    ))
    df[df_issuer_col] = df[df_issuer_col].fillna(df['issuer_by_ticker'])
    print("number of holdings with issuer after adding issuers by ticker: {}".format(
        df[df_issuer_col].notnull().sum()
    ))
    return df


def add_LEI_by_isin(df, mapping, df_isin_col):
    df_with_lei = pd.merge(
        left=df,
        right=mapping,
        left_on=df_isin_col,
        right_index=True,
        how='left'
    )
    print("ISINs with matching LEI: {} out of total rows: {}".format(
        df_with_lei["LEI"].notnull().sum(),
        df_with_lei.shape[0]
    ))
    return df_with_lei


# Matching functions: holdings with prev, TLV list, FFF list
def match_holdings_with_prev(holdings, prev, holdings_isin_col, holdings_corp_or_issuer_col):
    # 1. matching by ISIN / security number
    print("\n1. matching to previously classified by ISIN / security number")
    prev_sec_num = prev.groupby("security_num").first()
    holdings_prev_by_isin = pd.merge(left=holdings,
                                     right=prev_sec_num['is_fossil'],
                                     left_on=holdings_isin_col,
                                     right_index=True,
                                     how='left'
                                     )
    holdings_prev_by_isin.rename({"is_fossil": "is_fossil_prev_ISIN"}, axis=1, inplace=True)
    print("\nprevious is_fossil coverage")
    print("ISINs previously classified: {} out of total holdings: {}".format(
        holdings_prev_by_isin["is_fossil_prev_ISIN"].notnull().sum(),
        holdings_prev_by_isin.shape[0]
    ))
    # 2. by issuer number
    print("\n2. matching to previously classified by issuer number")
    prev_issuer = prev.groupby("מספר מנפיק").first()
    holdings_prev_by_issuer = pd.merge(left=holdings_prev_by_isin,
                                       right=prev_issuer['is_fossil'],
                                       left_on="מספר מנפיק",
                                       right_index=True,
                                       how='left'
                                       )
    holdings_prev_by_issuer.rename({"is_fossil": "is_fossil_prev_issuer"}, axis=1, inplace=True)
    print("issuers previously classified: {} out of total holdings: {}".format(
        holdings_prev_by_issuer["is_fossil_prev_issuer"].notnull().sum(),
        holdings_prev_by_issuer.shape[0]
    ))
    # 3. by LEI - (Legal Entity Identifier, international)
    print("\n3. matching to previously classified by LEI")
    prev_LEI = prev.groupby("LEI").first()
    holdings_prev = pd.merge(left=holdings_prev_by_issuer,
                             right=prev_LEI['is_fossil'],
                             left_on="LEI",
                             right_index=True,
                             how='left'
                             )
    holdings_prev.rename({"is_fossil": "is_fossil_prev_LEI"}, axis=1, inplace=True)
    print("LEIs previously classified: {} out of total holdings: {}".format(
        holdings_prev["is_fossil_prev_LEI"].notnull().sum(),
        holdings_prev.shape[0]
    ))
    return holdings_prev


def match_holdings_with_tlv(holdings, tlv, holdings_corp_or_issuer_col):
    # join on issuer number
    holdings_with_tlv = pd.merge(left=holdings,
                                 right=tlv[['מספר מנפיק', 'רשימה שחורה']],
                                 right_on='מספר מנפיק',
                                 left_on="מספר מנפיק",
                                 how='left'
                                 )
    holdings_with_tlv.rename({"רשימה שחורה": "is_fossil_il_list_issuer"}, axis=1, inplace=True)
    holdings_with_tlv["is_fossil_il_list_issuer"].value_counts(dropna=False)
    print("\nTLV list is_fossil coverage")
    print("classified: {} out of total holdings: {}".format(
        holdings_with_tlv["is_fossil_il_list_issuer"].notnull().sum(),
        holdings_with_tlv.shape[0]
    ))
    return holdings_with_tlv


def match_holdings_with_fff_by_ticker(
        holdings,
        fff,
        holdings_ticker_col,
        holdings_company_col,
        fff_company_col="Company",
        match_threshold=80):
    holdings_without_ticker = holdings[holdings[holdings_ticker_col].isnull()]
    print("Holdings without ticker: {}".format(holdings_without_ticker.shape[0]))
    holdings_with_ticker = holdings[holdings[holdings_ticker_col].notnull()]
    print("Holdings with ticker: {}".format(holdings_with_ticker.shape[0]))
    holdings_with_ticker["clean_ticker"] = holdings_with_ticker[holdings_ticker_col].map(lambda s: clean_ticker(s))
    fff["clean_ticker"] = fff["Tickers"].map(lambda s: clean_ticker(s))
    fff = fff[fff["clean_ticker"].notnull()]
    fff_one_per_ticker = fff.groupby(["clean_ticker", fff_company_col]).first().reset_index()
    fff_one_per_ticker["clean_ticker"] = id_col_clean(fff_one_per_ticker["clean_ticker"])
    fff_one_per_ticker = fff_one_per_ticker[fff_one_per_ticker["clean_ticker"].notnull()]
    fff_one_per_ticker = fff_one_per_ticker.set_index('clean_ticker')
    holdings_with_ticker["clean_ticker"] = id_col_clean(holdings_with_ticker["clean_ticker"])
    holdings_with_fff_by_ticker = pd.merge(
        holdings_with_ticker,
        fff_one_per_ticker[[fff_company_col, 'fff_fossil_any']],
        left_on="clean_ticker",
        right_index=True,
        how='left'
    )
    holdings_with_fff_by_ticker.rename({fff_company_col: 'fff_company_by_ticker'}, axis=1, inplace=True)
    # adding fuzzy matching between holdings company name and fff company name to discard false positives by ticker
    holdings_with_fff_by_ticker['ticker_company_match_score'] = holdings_with_fff_by_ticker.apply(
        lambda row: company_names_match_score(
            row,
            holdings_company_col=holdings_company_col,
            fff_company_col='fff_company_by_ticker'
        ),
        axis='columns'
    )
    # take ticker matches with maximal company name match
    got_ticker_matches = holdings_with_fff_by_ticker[holdings_with_fff_by_ticker['fff_company_by_ticker'].notnull()]
    no_ticker_matches = holdings_with_fff_by_ticker[holdings_with_fff_by_ticker['fff_company_by_ticker'].isnull()]
    # done by desc sorting by company score and then de-duping to keep the rows with max score per holding
    got_ticker_matches = got_ticker_matches.sort_values(
        'ticker_company_match_score', ascending=False).drop_duplicates(
        got_ticker_matches.columns.drop(['fff_company_by_ticker', 'fff_fossil_any', 'ticker_company_match_score'])
    )
    holdings_with_fff_by_ticker = pd.concat([got_ticker_matches, no_ticker_matches])
    holdings_with_fff_by_ticker["is_fossil_fff_ticker"] = holdings_with_fff_by_ticker.apply(
        lambda row: row['fff_fossil_any'] if row['ticker_company_match_score'] > match_threshold else np.nan,
        axis='columns'
    )
    # rename columns
    holdings_with_fff_by_ticker = holdings_with_fff_by_ticker.rename({"fff_fossil_any": "fff_by_ticker_fossil"}, axis=1)
    holdings_with_fff_by_ticker = pd.concat([holdings_with_fff_by_ticker, holdings_without_ticker])
    print("Matching by Ticker coverage:")
    print("classified: {} out of total holdings: {}".format(
        holdings_with_fff_by_ticker["is_fossil_fff_ticker"].notnull().sum(),
        holdings_with_fff_by_ticker.shape[0]
    ))
    return holdings_with_fff_by_ticker


def best_match(s, l, first_word_thresh=95):
    s = str(s)
    # if there's a perfect match, it's the winner
    if s in l:
        return s, 100
    # start with matching the first word (most indicative)
    if len(s) > 0:
        first_word_matches = process.extract(s.split()[0], l, scorer=fuzz.partial_ratio, limit=10)
    else:
        return '', 0
    # go over candidates with good first word match, get fuzzy match score for each and choose winner
    max_agg_score = 0
    winner = ''
    for m in first_word_matches:
        if m[1] > first_word_thresh:
            agg_score = (
                fuzz.ratio(s, m[0]) +
                fuzz.partial_ratio(s, m[0]) +
                fuzz.token_sort_ratio(s, m[0]) +
                fuzz.token_set_ratio(s, m[0]) +
                fuzz.partial_token_sort_ratio(s, m[0]) +
                fuzz.partial_token_set_ratio(s, m[0])
            )
            if agg_score > max_agg_score:
                max_agg_score = agg_score
                winner = m[0]
    # normalize score to be 0-100
    final_score = max_agg_score / 6
    return winner, final_score


def match_holdings_with_fff_by_company_name(
        holdings,
        fff,
        common_words_in_company,
        holdings_company_col,
        fff_company_col="Company",
        min_match_threshold=60,
        is_fossil_match_threshold=90
        ):
    # prepare company names for fuzzy matching
    # remove common words (LTD, Corp etc.)
    holdings["company_clean"] = holdings[holdings_company_col].map(lambda s: clean_company(s))
    holdings["company_clean"] = remove_common_words(holdings["company_clean"], common_words_in_company)
    # TODO: maybe use ASA, PLC, INC etc. as separator? remove everything after separator if got >= n (3?) words
    holdings_company_names = holdings["company_clean"].dropna().str.upper().str.strip().unique()
    fff["company_clean"] = remove_common_words(fff[fff_company_col], common_words_in_company)
    fff["company_clean"] = fff["company_clean"].str.upper().str.strip()
    fff_company_names = fff["company_clean"].dropna().unique()
    # fuzzy matching company names
    print("\n** fuzzy matching company names ** (this could take a few minutes)")
    agg_matches = {}
    for c in holdings_company_names:
        agg_matches[c] = best_match(c, fff_company_names)
    agg_fuzzy_results = pd.DataFrame(agg_matches).transpose()
    agg_fuzzy_results.rename({0: 'fff_by_name', 1: 'company_name_match_score'}, axis=1, inplace=True)
    agg_fuzzy_results = agg_fuzzy_results[agg_fuzzy_results['company_name_match_score'] > min_match_threshold]
    # join back to fff to get fff_fossil_any
    fff_company_with_fff_fossil_any = fff.groupby('company_clean').first()
    agg_fuzzy_results = pd.merge(
        left=agg_fuzzy_results,
        right=fff_company_with_fff_fossil_any['fff_fossil_any'],
        left_on='fff_by_name',
        right_index=True,
        how='left'
    )
    # add fuzzy match results to holdings
    holdings_with_fuzzy = pd.merge(
        left=holdings,
        right=agg_fuzzy_results,
        left_on="company_clean",
        right_index=True,
        how='left'
    )
    holdings_with_fuzzy["is_fossil_company_name"] = holdings_with_fuzzy.apply(
        lambda row: row['fff_fossil_any'] if row['company_name_match_score'] > is_fossil_match_threshold else np.nan,
        axis='columns'
    )
    # rename columns
    holdings_with_fuzzy = holdings_with_fuzzy.rename({'fff_fossil_any': 'fff_by_name_fossil'}, axis=1)
    # drop redundant columns
    if 'company_name_cut_ticker' in holdings_with_fuzzy.columns:
        holdings_with_fuzzy = holdings_with_fuzzy.drop(['company_name_cut_ticker'], axis=1)
    print("Matching by Company Name coverage:")
    print("classified: {} out of total holdings: {}".format(
        holdings_with_fuzzy["is_fossil_company_name"].notnull().sum(),
        holdings_with_fuzzy.shape[0]
    ))
    return holdings_with_fuzzy


# is_fossil consolidation - using multiple is_fossil_x flags to get is_fossil
def consolidate_is_fossil(df):
    # produces final is_fossil flag, based on all the sub flags
    # adds is_fossil_reason and is_fossil_certainty (=1 for all except FFF fuzzy matching)
    is_fossil_cols = [c for c in df.columns if c.startswith("is_fossil")]
    # if any fossil flag is true, is_fossil := True
    # if all available fossil flags are false, is_fossil := False
    # if there are no available fossil flags, is_fossil := null
    # TODO: handle rows with mixed flags (0 and 1)
    df["is_fossil"] = df[is_fossil_cols].max(axis=1)
    print("\n***** Final Results before propagation *****")
    print("is_fossil coverage:")
    print(df["is_fossil"].value_counts(dropna=False))
    return df


# propagate is_fossil across same identity (ISIN, LEI)
def propagate_is_fossil(df, propagate_by_col):
    # use freshly classified holdings to classify others with similar ISINs or LEIs
    print("\nPropagating by {}".format(propagate_by_col))
    df = df.reset_index(drop=True)
    prop_col_null = df[df[propagate_by_col].isnull()]
    prop_col_not_null = df[df[propagate_by_col].notnull()]
    grouped_by_prop_col = prop_col_not_null.sort_values(propagate_by_col).groupby(propagate_by_col)
    # HAVING different is_fossil values, including nulls
    # fossil_partially_missing = grouped_by_prop_col.filter(lambda x: x["is_fossil"].nunique(dropna=False) > 1)
    # if len(fossil_partially_missing) >0:
    #     print("\nHAVING different is_fossil values within group, including nulls (partially missing classification)")
    #     print(grouped_by_prop_col.filter(lambda x: x["is_fossil"].nunique(dropna=False) > 1))
    # HAVING both is_fossil=0 and is_fossil=1 values
    fossil_ambiguous = grouped_by_prop_col.filter(lambda x: 0 < x["is_fossil"].mean() < 1)
    # TODO: Warning - multiple is_fossil values for the same entity
    if len(fossil_ambiguous) > 0:
        print("\nHAVING both is_fossil=0 and is_fossil=1 values within group")
        print(fossil_ambiguous)
    # propagate mean to missing is_fossil
    prop_col_not_null['is_fossil'] = grouped_by_prop_col['is_fossil'].transform(lambda x: x.fillna(x.mean()))
    result = pd.concat([prop_col_not_null, prop_col_null])
    print("\nis_fossil coverage before propagation by {}:".format(propagate_by_col))
    print(df["is_fossil"].value_counts(dropna=False))
    print("\nis_fossil coverage after propagation by {}:".format(propagate_by_col))
    print(result["is_fossil"].value_counts(dropna=False))
    return result


# TODO: upload csv to Google Drive or other repository
def output(df, output_path):
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print("\nWriting results to {}".format(output_path))


def classify_holdings(tlv_path="/Users/urimarom/Downloads/ניתוח כל החברות בבורסה מעודכן.xlsx",
         prev_class_path="/Users/urimarom/Downloads/fossil_classification_until_q3_2020.csv",
         isin2lei_path="/Users/urimarom/Downloads/ISIN_LEI_20210317.csv",
         holdings_path="/Users/urimarom/Downloads/החזקות קרנות נאמניות - with manual.csv",
         holdings_corp_or_issuer_col="מספר מנפיק",
         holdings_ticker_col="TICKER",
         holdings_company_col="Instrument Name",
         sheet_num=0
         ):
    # 1. prepare holdings file for classification
    holdings, holdings_isin_col, holdings_il_corp_col = prepare_holdings(holdings_path, sheet_num=sheet_num)
    # If ticker exists, remove ticker information from instrument name
    if holdings_ticker_col:
        holdings = clean_instrument_from_ticker(holdings, holdings_company_col, holdings_ticker_col)
        holdings_company_col = "company_name_cut_ticker"
    # 2. prepare mapping files: TLV security number to issuer & isin to LEI for international holdings
    tlv_s2i = prepare_tlv_sec_num_to_issuer(fetch_latest_tlv_sec_num_to_issuer())
    isin2lei = fetch_latest_isin2lei(isin2lei_path)
    # 3. add issuer and LEI to holdings file
    holdings_with_issuer = add_tlv_issuer_by_col(holdings, tlv_s2i, holdings_join_col=holdings_isin_col,mapping_join_col="""מס' ני"ע""")
    holdings_with_issuer = add_tlv_issuer_by_col(holdings_with_issuer, tlv_s2i, holdings_join_col=holdings_il_corp_col, mapping_join_col="מספר תאגיד")
    if holdings_ticker_col:
        holdings_with_issuer = add_tlv_issuer_by_ticker(
            holdings_with_issuer,
            tlv_s2i,
            df_isin_col=holdings_isin_col,
            df_issuer_col="מספר מנפיק",
            df_ticker_col=holdings_ticker_col,
            mapping_heb_ticker_col="סימול(עברית)",
            mapping_eng_ticker_col="סימול(אנגלית)"
        )
    holdings_with_LEI = add_LEI_by_isin(holdings_with_issuer, isin2lei, df_isin_col=holdings_isin_col)
    # 4. prepare previously classified as is_fossil
    prev_class = prepare_prev_class(fetch_latest_prev_classified(prev_class_path))
    # 5. add issuer and LEI for previously classified
    # to be removed - prev_class file should already have issuer_number and LEI
    prev_class = add_tlv_issuer_by_col(prev_class, tlv_s2i, holdings_join_col="security_num", mapping_join_col="""מס' ני"ע""")
    prev_class = add_LEI_by_isin(prev_class, isin2lei, df_isin_col="security_num")
    output(prev_class, "prev with added issuer and LEI.csv")
    # 6. match holdings with previously classified - by ISIN, issuer or LEI
    holdings_with_prev = match_holdings_with_prev(
        holdings_with_LEI,
        prev_class,
        holdings_isin_col,
        "מספר מנפיק"
    )
    tlv = prepare_tlv(fetch_latest_tlv_list(tlv_path))
    holdings_with_tlv = match_holdings_with_tlv(holdings_with_prev, tlv, holdings_corp_or_issuer_col)
    # 7. get Fossil Free Funds company list, transform to one row per ticker symbol
    fff_all = fetch_latest_fff_list()
    fff = prepare_fff(fff_all)
    # 8. match holdings with FFF
    # TODO: if needed, add Ticker per holding using open FIGI API (only if company name isn't enough)
    # 8a. match by ticker if exists
    if holdings_ticker_col:
        holdings_with_fff_by_ticker = match_holdings_with_fff_by_ticker(
            holdings_with_tlv,
            fff,
            holdings_ticker_col=holdings_ticker_col,
            holdings_company_col=holdings_company_col
        )
    else:
        holdings_with_fff_by_ticker = holdings_with_tlv
    # output(holdings_with_fff_by_ticker, "after_ticker_" + output_path)
    # 8b. match by fuzzy company name
    # prepare common words to ignore while matching
    common = get_common_words_in_company_name(
        holdings_with_fff_by_ticker,
        fff_all,
        holdings_company_col=holdings_company_col,
        fff_company_col="Company"
    )
    holdings_with_fff_by_company_name = match_holdings_with_fff_by_company_name(
        holdings_with_fff_by_ticker,
        fff,
        common_words_in_company=common,
        holdings_company_col=holdings_company_col,
        fff_company_col="Company"
    )
    # TODO: inner matching - consolidate to issuer based on ISIN
    # (doable in the US - without the last characters, check about the others)
    # 9. calculate is_fossil (if any of the is_fossil_* flags exists, take it)
    holdings_final = consolidate_is_fossil(holdings_with_fff_by_company_name)
    # output(holdings_final, "debug_" + output_path)
    # 10. propagate is_fossil across ISIN and LEI (fill in missing is_fossil according to existing ones within group)
    holdings_propagate_is_fossil = propagate_is_fossil(holdings_final, holdings_isin_col)
    holdings_propagate_is_fossil = propagate_is_fossil(holdings_propagate_is_fossil, "LEI")
    # output path = input path with 'with fossil classification' added
    output_path = ''.join(holdings_path.split('.')[:-1]) + ' with fossil classification.' + holdings_path.split('.')[-1]
    output(holdings_propagate_is_fossil, output_path)
    return

holdings_path = "/Users/urimarom/Downloads/holdings reports/2021q2 reports/quarterly_holdings_for_classification.xlsx"
tlv_path = "/Users/urimarom/Downloads/כל החברות 2021 -אוגוסט 2021.xlsx"
prev_class_path = "/Users/urimarom/Downloads/holdings reports/2021q2 reports/חשיפה לפוסיליים - סיווגי רבעונים קודמים - all up to 2021Q1.csv"
isin2lei_path = "/Users/urimarom/Downloads/ISIN_LEI_20210928.csv"
holdings_corp_or_issuer_col = "מספר מנפיק"
holdings_ticker_col = None
holdings_company_col = "שם המנפיק/שם נייר ערך"
sheet_num = 1

classify_holdings(tlv_path,
     prev_class_path,
     isin2lei_path,
     holdings_path,
     holdings_corp_or_issuer_col,
     holdings_ticker_col,
     holdings_company_col,
     sheet_num
     )
# ***** Final Results *****
# is_fossil coverage:
# NaN    5949
# 0.0    3884
# 1.0     386

# ***** Final Results *****
# is_fossil coverage:
# NaN    5733
# 0.0    4081
# 1.0     405

# after adding issuer by ticker for TLV companies
# ***** Final Results *****
# is_fossil coverage:
# NaN    5748
# 0.0    4127
# 1.0     405
# bug - added duplicates

# bug fixed:
# is_fossil coverage:
# NaN    5698
# 0.0    4116
# 1.0     405

# after adding ticker match to FFF
# is_fossil coverage:
# NaN    5627
# 0.0    4115
# 1.0     477

# after adding LEI mapping to both previously classified and holdings
# is_fossil coverage:
# 0.0    4962
# NaN    4681
# 1.0     576

# after consolidation by LEI
# is_fossil coverage after propagation by ISIN and LEI:
# 0.0    5131
# NaN    4499
# 1.0     589

# after best_match bug fix:
# 0.0    5131
# NaN    4484
# 1.0     604
# Name: is_fossil, dtype: int64

# after adding non fossil companies from FFF list and better ticket matching
# 0.0    6906
# NaN    2722
# 1.0     591

# after better ticker and company name cleaning
# 0.0    7072
# NaN    2532
# 1.0     615