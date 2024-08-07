import pandas as pd
import numpy as np
import urllib.request as ur
import urllib
import time
from os import listdir
from os.path import isfile, join, getmtime
import re
from pathlib import Path
from enrich_holdings import *
import requests


def last_updated():
    from datetime import datetime  # Current date time in local system
    last_mod_time = getmtime(__file__)
    print(".py file last modified: {}".format(datetime.fromtimestamp(last_mod_time)))


def fetch_all_holdings_path():
    """Returns the relative path of the all_holdings.csv file

    :return: the relative path of all_holdings.csv file
    """
    return "data/downloaded reports/company reports/all_holdings.csv"


def fetch_all_company_holdings_cls_path():
    """Returns the relative path of the all_company_holdings_cls file

    :return: the relative path of all_company_holdings_cls file
    """
    return "data/all_company_holdings_cls"


def get_report_data_into_data_frame(from_year, from_q, to_year, to_q, report_type, system, report_status=1, corp=None):
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9,he;q=0.8',
        'Connection': 'keep-alive',
        'Origin': 'https://employersinfocmp.cma.gov.il',
        'Referer': 'https://employersinfocmp.cma.gov.il/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36',
        'content-type': 'application/json',
        'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
    }

    json_data = {
        'corporation': corp,
        'fromYear': from_year,
        'fromQuarter': from_q,
        'toYear': to_year,
        'toQuarter': to_q,
        'reportFromDate': None,
        'reportToDate': None,
        'investmentName': None,
        'reportType': report_type,
        'systemField': system,
        'statusReport': report_status,
    }

    response = requests.post(
        'https://employersinfocmp.cma.gov.il/api/PublicReporting/GetPublicReports',
        headers=headers,
        json=json_data,
    )
    reports = pd.DataFrame.from_dict(response.json())
    # add download links to response dataframe
    download_link_prefix = "https://employersinfocmp.cma.gov.il/api/PublicReporting/downloadFiles?IdDoc="
    download_link_suffix = "&extention=XLSX"
    reports["url"] = download_link_prefix + reports["DocumentId"].astype(str) + download_link_suffix
    print("number of reports for {} q{} until {} q{}: {}".format(from_year, from_q, to_year, to_q, reports.shape[0]))
    return reports


def get_reports_from_response(response_directory):
    """get reports dataframe from response.json within the response directory.
    Using the http response recorded while searching for reports here:
    https://employersinfocmp.cma.gov.il/#/publicreports

    :param response_directory: text, path of the directory with the response.
    :return: DataFrame: the response json as DataFrame, with added download_link
    """
    response_path = response_directory + "response.json"
    reports = pd.read_json(response_path)
    # add download links to response dataframe
    download_link_prefix = "https://employersinfocmp.cma.gov.il/api/PublicReporting/downloadFiles?IdDoc="
    download_link_suffix = "&extention=XLSX"
    reports["url"] = download_link_prefix + reports["DocumentId"].astype(str) + download_link_suffix
    print("Number of reports included in response.json: {}".format(reports.shape[0]))
    return reports


def add_filename_to_report(row):
    parent_corp_id = row["ParentCorpLegalId"]
    period_desc = row["ReportPeriodDesc"]
    y = period_desc[2:4]
    q = period_desc[-1]
    system = row["SystemName"]
    if (system == "פנסיה"):
        sys_abb = "pn"
    elif (system == "גמל"):
        sys_abb = "gm"
    elif (system == "ביטוח"):
        sys_abb = "in"
    elif (system == "מובטח תשואה"):
        sys_abb = "ca"
    filename = parent_corp_id + "_" + sys_abb + "_p_0" + q + y + "." + row["fileExt"]
    return filename


def download_reports(files_df, to_dir, sleep=6):
    """Download reports from files_df, with a delay in between downloads

    :param files_df: a DataFrame of reports
    :param to_dir: target directory for downloads
    :param sleep: number of seconds to wait between downloads, default=6
    :return:
    """
    # download files
    files_len = len(files_df)
    file_num = 1
    for index, row in files_df.iterrows():
        print("Downloading file {} out of {}".format(file_num, files_len), end="\r")
        try:
            url = row["url"]
            filename = to_dir + row["filename"]
            ur.urlretrieve(url, filename)
            time.sleep(sleep)
        except urllib.error.HTTPError as err:
            print("HTTP error {} when trying to download report {}".format(err.code, filename))
            print(row["ParentCorpName"], row["SystemName"], row["Name"], sep=" | ")
            continue
        finally:
            file_num += 1


def connect_to_gspreadsheets_api(json_keyfile_name):
    from oauth2client.service_account import ServiceAccountCredentials
    import gspread
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive.file',
             'https://www.googleapis.com/auth/drive']

    # Reading Credentails from ServiceAccount Keys file
    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_name, scope)

    # intitialize the authorization object
    gc = gspread.authorize(credentials)
    return gc


def add_document_id_by_cols(df, reports, system, id_cols):
    # convert all id cols to string
    for id_col in id_cols:
        df[id_col] = df[id_col].astype(str)
    # filter relevant system reports, convert ids to string
    system_reports = reports.loc[reports['SystemName'] == system]
    system_reports['ParentCorpLegalId'] = system_reports['ParentCorpLegalId'].astype(str)
    system_reports['ProductNum'] = system_reports['ProductNum'].astype(str)
    # merge
    df_with_doc_id = pd.merge(
        left=df,
        right=system_reports,
        how='left',
        left_on=id_cols,
        right_on=['ParentCorpLegalId', 'ProductNum']
    )
    cols_to_keep = [c for c in df.columns] + ['DocumentId']
    cols_to_keep = [col for col in df_with_doc_id.columns if col in cols_to_keep]
    result_sheet = df_with_doc_id[cols_to_keep]
    return result_sheet


def add_document_ids_to_all_sheets(gc, gss_url, reports):
    system_id_cols= {
        "פנסיה": ['NUM_HEVRA','ID_MASLUL_RISHUY']
        ,
        "גמל": ['NUM_HEVRA','ID']
        ,
        "ביטוח": ['NUM_HEVRA','ID_GUF']
    }
    # rename System for insurance reports if needed
    reports.loc[reports["SystemName"] == 'חיים ואובדן כושר עבודה', "SystemName"] = 'ביטוח'
    reports["DocumentId"] = reports["DocumentId"].astype(str)
    # iterate over worksheets and add document_id from reports
    gss = gc.open_by_url(gss_url)
    for sys in system_id_cols:
        ws = gss.worksheet(sys)
        df = pd.DataFrame(ws.get_all_records())
        df_with_document_id = add_document_id_by_cols(df, reports, sys, system_id_cols[sys])
        # converting to string to prevent error in update
        df_with_document_id = df_with_document_id.astype(str)
        ws.update([df_with_document_id.columns.values.tolist()] + df_with_document_id.values.tolist())


def fix_sheet_name(sheet_name):
    """Fix sheet names across files

    :param sheet_name: input sheet_name
    :return: fixed sheet_name
    """
    # remove spaces from sheet_name
    if type(sheet_name) == str:
        sheet_name = sheet_name.strip().replace("-", " - ").replace("  ", " ").replace('אגח', 'אג"ח')
    # map variations to the same format
    if sheet_name in ['לא סחיר - תעודות התחייבות ממשלת', 'לא סחיר- תעודות התחייבות ממשלתי']:
        return 'לא סחיר - תעודות התחייבות ממשלתי'
    elif sheet_name in ['עלות מתואמת אג"ח קונצרני ל', 'עלות מתואמת אג"ח קונצרני ל.סחי']:
        return 'עלות מתואמת אג"ח קונצרני ל.סחיר'
    elif sheet_name in ['עלות מתואמת אג"ח קונצרני ס', 'עלות מתואמת - אג"ח קונצרני סחיר', 'עלות מתואמת אג"ח קונצרני']:
        return 'עלות מתואמת אג"ח קונצרני סחיר'
    elif sheet_name in ['תעודות השתתפות בקרנות נאמנות']:
        return 'קרנות נאמנות'
    elif sheet_name in ['תעודות סל']:
        return 'קרנות סל'
    elif sheet_name in ['עלות מתואמת מסגרת אשראי ללווי', 'עלות מתואמת מסגרת אשראי ללווים']:
        return 'עלות מתואמת מסגרות אשראי ללווים'
    else:
        return sheet_name


def fix_col_name(col_name):
    """Fix column names across files

    :param col_name: column name to be fixed
    :return: fixed column name
    """
    # remove * from col_name, fix שיעור to שעור
    if type(col_name) == str:
        col_name = re.sub(r'[*:]+', '', col_name)
        col_name = col_name.replace('שיעור', 'שעור')
        col_name = col_name.replace('פידיון', 'פדיון')
        col_name = col_name.strip()
    # map variations to the same format
    if col_name in ['שם נ"ע', 'שם המנפיק / שם נייר ערך']:
        return 'שם המנפיק/שם נייר ערך'
    elif col_name in ['מספר נ"ע', 'מספר הנייר', 'מספר נייר']:
        return 'מספר ני"ע'
    elif col_name in ['פדיון/ריבית לקבל', 'פידיון/ריבית לקבל', 'פדיון/ ריבית לקבל', 'דיבידנד לקבל',
                      'פדיון/ ריבית/ דיבידנד לקבל', 'פדיון/ריבת לקבל']:
        return 'פדיון/ריבית/דיבידנד לקבל'
    elif col_name in ['שעור מנכסי השקעה', 'שעור מסך נכסי ההשקעה']:
        return 'שעור מסך נכסי השקעה'
    elif col_name in ['שעור מנכסי אפיק ה השקעה']:
        return 'שעור מנכסי אפיק ההשקעה'
    elif col_name in ['ספק המידע']:
        return 'ספק מידע'
    elif col_name in ['שווי הוגן', 'שווי שוק', 'שווי משוערך', 'עלות מתואמת', 'עלות מותאמת']:
        return 'שווי'
    elif col_name in ['נכס הבסיס']:
        return 'נכס בסיס'
    elif col_name in ['ענף משק']:
        return 'ענף מסחר'
    elif col_name in ['שעור התשואה במהלך התקופה']:
        return 'שעור תשואה במהלך התקופה'
    elif col_name in ['קונסורציום כן / לא', 'קונסורציום']:
        return 'קונסורציום כן/לא'
    elif col_name in ['שם המדרג']:
        return 'שם מדרג'
    elif col_name in ['שעור הריבית', 'תנאי ושעור ריבית', 'שעור ריבית ממוצע']:
        return 'שעור ריבית'
    else:
        return col_name


def clean_sheet(sheet, null_pct_thresh=0.5):
    """ remove columns and rows with > null_pct_thresh% nulls
    Args:
      sheet (DataFrame): sheet to be handled
      null_pct_thresh (float): threshold for row null removals

    Returns:
      DataFrame: sheet without empty columns and rows having > null_pct_thresh% nulls
    """
    # drop empty columns
    sheet = sheet.dropna(axis=1, how='all')
    num_cols = len(sheet.columns)
    sheet = sheet.dropna(axis=0, thresh=num_cols * null_pct_thresh)
    if sheet.empty:
        return sheet
    else:
        sheet.columns = sheet.iloc[0].str.strip().map(fix_col_name)
        # remove first 1 row (header) and columns with null header
        sheet = sheet.iloc[1:, sheet.columns.notnull()]
        return sheet


def ignore_sheets(report):
    """remove sheets that are not needed from a report

    :param report: report (sheet list)
    :return: report without ignored sheets
    """
    ignore_sheet_list = ['יתרת התחייבות להשקעה', 'סכום נכסי הקרן']
    #     ignore_pattern = r'^עלות'
    r = [s for s in report if (s not in ignore_sheet_list)]
    #          &  (not (re.match(ignore_pattern, s)))]
    return r


def get_filename_list(reports_path):
    """

    :param reports_path: the path of the downloaded reports
    :return: a list of report filenames
    """
    # get all reports from directory
    reports_fn_list = [join(reports_path, f) for f in listdir(reports_path)
                       if isfile(join(reports_path, f)) and not (f.startswith(".")) and f.endswith((".xlsx", ".xls"))]
    print("number of files to be pre-processed: {}".format(len(reports_fn_list)))
    return reports_fn_list


def pre_process_reports(reports_fn_list):
    """

    :param reports_fn_list: a list of report filenames to be processed
    :return: a DataFrame of column names' count per sheet, used to verify column name standardization
    """

    # 1. count sheet names across files, fix them
    sheet_names = {}
    for fn in reports_fn_list:
        print("Processing report: {}".format(fn), end="\r")
        try:
            report = pd.read_excel(fn, sheet_name=None, header=None)
            for k in report.keys():
                k = fix_sheet_name(k)  # following analysis of raw results
                if k in sheet_names:
                    sheet_names[k] += 1
                else:
                    sheet_names[k] = 1
        except:
            print("Something went wrong with report: {}".format(fn))
            reports_fn_list = [r for r in reports_fn_list if r != fn]
    print(sheet_names)
    # 2. count column names per sheet name
    column_names = {}
    for fn in reports_fn_list:
        report = pd.read_excel(fn, sheet_name=None, header=None)
        for sheet_name in ignore_sheets(report):
            fixed_sheet_name = fix_sheet_name(sheet_name)
            if fixed_sheet_name not in column_names:
                column_names[fixed_sheet_name] = {}
            sheet = clean_sheet(report[sheet_name])
            if not sheet.empty:
                for c in sheet.columns:
                    c = fix_col_name(c)  # following analysis of raw results
                    if c in column_names[fixed_sheet_name]:
                        column_names[fixed_sheet_name][c] += 1
                    else:
                        column_names[fixed_sheet_name][c] = 1
                if (sheet_name == 'מניות') & ('מספר מנפיק' not in sheet.columns):
                    print(fn)
                    print(sheet_name)
                    print("missing שם המנפיק/שם נייר ערך in file: {}, sheet: {}".format(fn, sheet_name))
    cols_matrix = pd.DataFrame(column_names)
    return cols_matrix[cols_matrix.index.notnull()]


def get_asset_allocation_from_summary_sheet(summary_sheet):
    """get asset allocation data from summary sheet

    :param summary_sheet: a summary data of a report
    :return: a DataFrame - could be empty
    """
    # remove header - first 1 row
    if len(summary_sheet) > 1:
        summary_sheet = summary_sheet.iloc[1:, ]
    # locate the word מזומנים as anchor for asset allocation
    anchor_loc = np.where(summary_sheet.apply(lambda col: col.str.contains('מזומנים', na=False), axis=1))
    if anchor_loc[0].size > 0:
        anchor_row_num = anchor_loc[0][0]
        anchor_col_num = anchor_loc[1][0]
        headers = summary_sheet.iloc[anchor_row_num:, anchor_col_num]
        # find the first null in the headers (where to stop parsing)
        if headers.loc[headers.isnull()].size > 0:
            headers_first_null = headers.loc[headers.isnull()].index[0]
        else:
            headers_first_null = headers.size
        headers_end_index = headers_first_null - 1
        asset_alloc = pd.DataFrame()
        asset_alloc["asset"] = summary_sheet.iloc[anchor_row_num:headers_end_index, anchor_col_num]
        asset_alloc["sum"] = summary_sheet.iloc[anchor_row_num:headers_end_index, anchor_col_num + 1]
        asset_alloc["pct"] = summary_sheet.iloc[anchor_row_num:headers_end_index, anchor_col_num + 2]
    else:
        print("No headers found :(((")
        return pd.DataFrame()
    return asset_alloc


def process_summary_sheets(reports_fn_list):
    """process all summary sheets from a reports filename list

    :param reports_fn_list: a list of report filenames
    :return: DataFrame of all summary sheets
    """
    all_summary_sheets_list = []
    list_len = len(reports_fn_list)
    rep_num = 1
    for fn in reports_fn_list:
        print("Processing report {} out of {}".format(rep_num, list_len), end="\r")
        try:
            sheet = pd.read_excel(fn, sheet_name="סכום נכסים", header=None)
            asset_alloc = get_asset_allocation_from_summary_sheet(sheet)
            if not asset_alloc.empty:
                # add report_id
                report_id = Path(fn).stem
                asset_alloc["report_id"] = report_id
                rep_num += 1
                all_summary_sheets_list.append(asset_alloc)
        except:
            print("Something went wrong with report: {}".format(fn))
            reports_fn_list = [r for r in reports_fn_list if r != fn]
    # moving concat out of the loop - better performance
    all_summary_sheets = pd.concat(all_summary_sheets_list, axis=0, ignore_index=True)
    all_summary_sheets = all_summary_sheets[all_summary_sheets["asset"].notnull()]
    all_summary_sheets["pct_num"] = all_summary_sheets["pct"].astype(str).str.replace(r'[\%\s-]', '')
    all_summary_sheets["pct_num"] = pd.to_numeric(all_summary_sheets["pct_num"], errors='ignore')
    all_summary_sheets["sum_num"] = pd.to_numeric(all_summary_sheets["sum"], errors='ignore')
    return all_summary_sheets


def get_totals(summary_sheets):
    """Get totals from summary sheets

    :param summary_sheets: a DataFrame of data allocations from reports summary sheets
    :return: the totals extracted from the reports summary sheets
    """
    totals = summary_sheets[summary_sheets["asset"].str.startswith('סך הכל נכסים')]
    print("Number of totals found: {}".format(totals["report_id"].nunique()))
    return totals


def extract_holdings(reports_fn_list):
    """extract holdings from reports

    :param reports_fn_list: list of reports filenames
    :return: DataFrame: unified holdings from all reports
    """
    all_holdings_list = []
    list_len = len(reports_fn_list)
    rep_num = 1
    for fn in reports_fn_list:
        print("Processing report {} out of {}".format(rep_num, list_len), end="\r")
        try:
            report = pd.read_excel(fn, sheet_name=None, header=None)
            # add report_id
            report_id = Path(fn).stem
            for sheet_name in ignore_sheets(report):
                fixed_sheet_name = fix_sheet_name(sheet_name)
                sheet = clean_sheet(report[sheet_name])
                if not sheet.empty:
                    sheet_df = sheet
                    sheet_df["report_id"] = report_id
                    # add holding_type column
                    sheet_df["holding_type"] = fixed_sheet_name
                    all_holdings_list.append(sheet_df)
            rep_num += 1
        except:
            print("Something went wrong with report: {}".format(fn))
            reports_fn_list = [r for r in reports_fn_list if r != fn]
    all_holdings = pd.concat(all_holdings_list, axis=0, ignore_index=True)
    all_holdings["report_id"] = all_holdings["report_id"].astype(str)
    return all_holdings


def no_holding_num_types():
    """holding types without expected holding number

    :return: a list of holding types without expected holding number
    """
    return ['זכויות מקרקעין', 'השקעה בחברות מוחזקות', 'השקעות אחרות']


def holdings_dtypes():
    """Return holdings dtypes

    :return: holding dtypes Dict
    """
    return {
        'מספר ני"ע': str,
        'מספר מנפיק': str,
        'מספר תאגיד': str,
        'ISIN': str,
        'LEI': str,
        'report_id': str,
        'ParentCorpLegalId': str,
        'ProductNum': str
    }


def is_number(s):
    """checks if a string is a number

    :param s: string
    :return: true if s is a float, false otherwise
    """
    try:
        float(s)
        return True
    except ValueError:
        return False


def clean_holdings(holdings):
    """ cleans all_holdings DataFrame, removing non-relevant rows and columns

    :param holdings: DataFrame
    :return: cleaned holdings DataFrame
    """
    # remove holdings with no name
    holdings['שם המנפיק/שם נייר ערך'] = holdings['שם המנפיק/שם נייר ערך'].astype('str')
    holdings_clean = holdings[
        holdings['שם המנפיק/שם נייר ערך'].str.replace("0", "").str.replace("nan", "").str.strip() != ""
                              ]
    # remove "total" lines
    total_lines = (holdings_clean['שם המנפיק/שם נייר ערך'].str.startswith('סה"כ')) & (
        holdings_clean['מספר ני"ע'].isnull())
    total_lines = total_lines | (holdings_clean['שם המנפיק/שם נייר ערך'].str.startswith('סה"כ'))
    holdings_clean = holdings_clean[~total_lines]
    # removing holdings with no num when applicable
    no_holding = no_holding_num_types()
    missing_holding_num_lines = (~holdings_clean["holding_type"].isin(no_holding)) & (
        holdings_clean['מספר ני"ע'].isnull())
    holdings_clean = holdings_clean[~missing_holding_num_lines]
    # remove holdings with no data texts
    no_data_texts = ['הגעת לשדה האחרון בשורה זו', 'תא ללא תוכן, המשך בתא הבא']
    missing_text_data_in_security_num =  (holdings_clean['מספר ני"ע'].isin(no_data_texts))
    holdings_clean = holdings_clean[~missing_text_data_in_security_num]
    # remove lines with שווי that is not a number
    holdings_clean = holdings_clean[holdings_clean['שווי'].map(is_number)]
    print("\nbefore cleaning: {}\n after cleaning: {}".format(len(holdings), len(holdings_clean)))
    # remove redundant columns
    cols_to_keep = [
        'שם המנפיק/שם נייר ערך', 'מספר ני"ע', 'מספר מנפיק', 'דירוג', 'שם מדרג',
        'סוג מטבע', 'שעור ריבית', 'תשואה לפדיון', 'שווי',
        'שעור מנכסי אפיק ההשקעה', 'שעור מסך נכסי השקעה', 'report_id',
        'holding_type', 'זירת מסחר', 'תאריך רכישה', 'מח"מ', 'ערך נקוב', 'שער',
        'פדיון/ריבית/דיבידנד לקבל', 'שעור מערך נקוב מונפק', 'ספק מידע',
        'ענף מסחר', 'נכס בסיס', 'קונסורציום כן/לא', 'תאריך שערוך אחרון',
        'אופי הנכס', 'שעור תשואה במהלך התקופה', 'כתובת הנכס', 'ריבית אפקטיבית'
    ]
    holdings_clean = holdings_clean[cols_to_keep]
    return holdings_clean


def add_report_data(holdings, reports):
    """Add fund and company data for the holdings from reports - fund type, parent corp & date of report

    :param holdings: DataFrame of holdings
    :param reports: DataFrame of reports
    :return: DataFrame of holdings with report data
    """
    #
    report_cols = ['SystemName',
                   'ParentCorpName', 'ParentCorpLegalId',
                   'ProductNum', 'Name', 'ShortName',
                   'StatusDate', 'ReportPeriodDesc']
    reports_with_doc_id = reports[reports["DocumentId"].notnull()]
    reports_with_doc_id = reports_with_doc_id.set_index('DocumentId')[report_cols]
    reports_with_doc_id.index = reports_with_doc_id.index.astype('str')
    reports_with_doc_id['ParentCorpLegalId'] = reports_with_doc_id['ParentCorpLegalId'].astype('str')
    # 1. merge holdings with report data when available
    holdings = holdings.merge(reports_with_doc_id,
                              left_on='report_id',
                              right_index=True,
                              how='left'
                              )
    # 2. Handle manually added reports
    # identify manually added reports by report_id (filename)
    manually_added_reports_mask = holdings["report_id"].str.contains("_")
    if manually_added_reports_mask.sum() > 0:
        manual_report_ids = holdings.loc[manually_added_reports_mask, "report_id"]
        # update corp_id
        holdings.loc[
            manually_added_reports_mask,
            "ParentCorpLegalId"
        ] = manual_report_ids.str.split("_").str[0]

        # update System
        holdings.loc[
            manually_added_reports_mask,
            "SystemName"
        ] = manual_report_ids.str.split("_").str[1].str[0].map({
            "b": "ביטוח",
            "p": "פנסיה",
            "g": "גמל"
        })

        # update ProductNum
        holdings.loc[
            manually_added_reports_mask,
            "ProductNum"
        ] = manual_report_ids.str.split("_").str[1].str[1:]
        # replace "sum" with 0 for manually added reports
        holdings.loc[holdings["ProductNum"] == 'sum', 'ProductNum'] = 0

        # update ReportPeriodDesc
        quarter = manual_report_ids.str.split("_").str[2].str[1]
        report_year = "20" + manual_report_ids.str.split("_").str[2].str[2:]
        holdings.loc[manually_added_reports_mask, "ReportPeriodDesc"] = report_year + " רבעון " + quarter

        # update corp details for manually added reports
        corps = reports[["ParentCorpLegalId", "ParentCorpName"]].set_index("ParentCorpLegalId").drop_duplicates()
        corps.index = corps.index.astype('str')

        updated = holdings[manually_added_reports_mask].merge(
            corps,
            left_on="ParentCorpLegalId",
            right_index=True,
            how='left',
            suffixes=('_y', '')
        )
        updated.drop(updated.filter(regex='_y$').columns.tolist(), axis=1, inplace=True)
        # important! keep columns order
        holdings.loc[manually_added_reports_mask, holdings.columns] = updated[holdings.columns]
    return holdings


def get_latest_fossil_classifications(prev_cls_fn):
    """Get the latest fossil classifications from a previous classifications file
    which contains all previous classifications

    :param prev_cls_fn: previous classifications filename
    :return: 2 DataFrames: latest classification per security_num and per ISIN
    """
    prev_csv = pd.read_csv(prev_cls_fn, parse_dates=['classification_date'])
    prev_csv['מספר ני"ע'] = prev_csv['מספר ני"ע'].astype('str')
    prev_csv['ISIN'] = prev_csv['ISIN'].astype('str').str.upper().str.strip()
    # 1. get latest classification (most updated) per Israeli security num
    latest_cls_by_sec_num = prev_csv.drop_duplicates(subset=['מספר ני"ע'])
    latest_cls_by_sec_num = latest_cls_by_sec_num[['מספר ני"ע', "is_fossil"]].set_index('מספר ני"ע')
    print("previously classified Israeli security nums by is_fossil:")
    print(latest_cls_by_sec_num["is_fossil"].value_counts(dropna=False))
    # 2. get latest classification (most updated) per ISIN
    latest_cls_by_ISIN = prev_csv.drop_duplicates(subset=['ISIN'])
    latest_cls_by_ISIN = latest_cls_by_ISIN[['ISIN', "is_fossil"]].set_index('ISIN')
    print("previously classified ISINs by is_fossil:")
    print(latest_cls_by_ISIN["is_fossil"].value_counts(dropna=False))
    return latest_cls_by_sec_num, latest_cls_by_ISIN


def add_fossil_classifications(holdings, fossil_cls_by_il_sec_num, fossil_cls_by_ISIN,
                               sec_num_col='מספר ני"ע',
                               value_col='שווי'):
    """Add fossil classifications to a holding file

    :param holdings: a holding file
    :param fossil_cls_by_il_sec_num: fossil classification, one row per security_num
    :param fossil_cls_by_ISIN: fossil classification, one row per ISIN
    :param sec_num_col: name of column with security number and ISIN (taken from reports as is)
    :param value_col: name of column with holding value
    :return: holding file with added classification and fossil sum columns
    """
    # 1. separate holdings with no holding number
    link_by_sec_num = (
            (holdings[sec_num_col].notnull()) &
            # ignore Israeli sec num for holding types where it should be ignored
            (~holdings["holding_type"].isin(ignore_id_types_holding_type()['מספר ני"ע']))
    )
    holdings_with_num = holdings[link_by_sec_num]
    holdings_no_num = holdings[~link_by_sec_num]
    print("all_holdings: {}".format(len(holdings)))
    print("having holding number: {}".format(len(holdings_with_num)))
    print("without holding number: {}".format(len(holdings_no_num)))
    # 2a. add fossil classification based on Israeli security num
    # clean join columns
    holdings_with_num[sec_num_col] = holdings_with_num[sec_num_col].astype('str').str.strip().str.upper()
    fossil_cls_by_il_sec_num.index = fossil_cls_by_il_sec_num.index.astype('str').str.strip()
    holdings_cls = holdings_with_num.merge(fossil_cls_by_il_sec_num,
                                           left_on=sec_num_col,
                                           right_index=True,
                                           how='left'
                                           )
    print("Holdings after fossil classification by Israeli security num:")
    print(holdings_cls["is_fossil"].value_counts(dropna=False))
    # 2b. add fossil classification based on ISIN
    # clean join columns
    fossil_cls_by_ISIN.index = fossil_cls_by_ISIN.index.astype('str').str.strip()
    holdings_cls = holdings_cls.merge(fossil_cls_by_ISIN,
                                      left_on=sec_num_col,
                                      right_index=True,
                                      how='left',
                                      suffixes=['', '_new']
                                      )
    holdings_cls["is_fossil"] = holdings_cls["is_fossil"].fillna(holdings_cls["is_fossil_new"])
    holdings_cls.drop(["is_fossil_new"], axis=1, inplace=True)
    print("Holdings after fossil classification by ISIN:")
    print(holdings_cls["is_fossil"].value_counts(dropna=False))
    # 3. add fossil sum שווי פוסילי
    holdings_cls = add_fossil_sum(holdings_cls, value_col)
    print("total fossil sum: {}".format(holdings_cls["שווי פוסילי"].sum()))
    holdings_cls = pd.concat([holdings_cls, holdings_no_num])
    print("holdings count before classification: {}".format(len(holdings)))
    print("holdings count after classification: {}".format(len(holdings_cls)))
    return holdings_cls


def concat_from_csv_by_path(all_holdings_path, new_holdings_path):
    """concat two csv into DataFrame

    :param all_holdings_path: all holdings path, CSV
    :param new_holdings_path: new holdings path, CSV
    :return: DataFrame of all holdings + new holdings
    """
    all_holdings = pd.read_csv(all_holdings_path, dtype=holdings_dtypes())
    new_holdings = pd.read_csv(new_holdings_path, dtype=holdings_dtypes())
    return pd.concat([all_holdings, new_holdings])
