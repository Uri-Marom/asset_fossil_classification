import pandas as pd
import enrich_holdings as eh

df = pd.read_excel("/Users/urimarom/Downloads/2021q1 reports/quarterly_holdings_for_classification.xlsx", sheet_name=3)
a = eh.find_id_cols(df)
print(a)

#
#
# holdings_path = "/Users/urimarom/Downloads/2021q1 reports/quarterly_holdings_for_classification.xlsx"
# tlv_path = "/Users/urimarom/Downloads/ניתוח כל החברות בבורסה מעודכן.xls"
# prev_class_path = "/Users/urimarom/Downloads/חשיפה לפוסיליים - סיווגי רבעונים קודמים - up to 2020Q4.csv"
# isin2lei_path = "/Users/urimarom/Downloads/ISIN_LEI_20210625.csv"
# holdings_corp_or_issuer_col = "מספר מנפיק"
# holdings_ticker_col = None
# holdings_company_col = "שם המנפיק/שם נייר ערך"
# sheet_num = 2
#
# classify_holdings(tlv_path,
#      prev_class_path,
#      isin2lei_path,
#      holdings_path,
#      holdings_corp_or_issuer_col,
#      holdings_ticker_col,
#      holdings_company_col,
#      sheet_num
#      )