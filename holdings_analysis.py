import numpy as np
import pandas as pd
from enrich_holdings import *


def get_major_institutions_list():
    """get a list of major institutional investors

    :return: a list of major institutional investors
    """
    institutions = [
        'מנורה',
        'מגדל',
        'הראל',
        'מיטב',
        'אלטשולר',
        'מור',
        'הפניקס',
        'כלל',
        'פסגות',
        'הלמן',
        # 'אנליסט',
        'ילין'
    ]
    return institutions


def sum_and_fossil_sum_by_group(holdings, group):
    """get sum and fossil sum for holdings by group

    :param holdings: holdings DataFrame
    :param group: a list of columns to group by
    :return: sum and fossil_sum by group
    """
    sums = pd.DataFrame(holdings.groupby(group, dropna=False).agg(
        {'שווי': 'sum', 'שווי פוסילי': 'sum'})
    ).reset_index()
    return sums


def get_summary(holdings, group_col, *additional_group_cols):
    """get summary stats grouped by 1 or more columns, e.g. Company, holding_type

    :param group_col: group by column
    :param additional_group_cols: additional group by columns
    :param holdings: holdings DataFrame
    :return: summary stats by group
    """
    group = [group_col] + [*additional_group_cols]
    summary = sum_and_fossil_sum_by_group(holdings, group)
    summary["שיעור פוסילי מסך הנכסים"] = 1.00 * summary["שווי פוסילי"] / summary["שווי"]

    # add summary of stocks and bonds only
    holdings_cls_trd_stocks_bonds = holdings[holdings["holding_type"].isin(['מניות', 'אג"ח קונצרני'])]
    summary_tradable_stocks_bonds_only = sum_and_fossil_sum_by_group(holdings_cls_trd_stocks_bonds, group)
    summary_tradable_stocks_bonds_only["שיעור פוסילי במניות ואגח קונצרני סחירים"] = (
            1.00 * summary_tradable_stocks_bonds_only["שווי פוסילי"] /
            summary_tradable_stocks_bonds_only["שווי"]
    )
    summary = summary.merge(summary_tradable_stocks_bonds_only,
                            left_on=group,
                            right_on=group,
                            how='left',
                            suffixes=['', ' במניות ואגח קונצרני סחירים']
                            )

    # add summary of non fossils
    holdings_cls_non_fossil_types = holdings[holdings["holding_type"].isin(get_non_fossil_holding_types())]
    summary_non_fossil_types = pd.DataFrame(
        holdings_cls_non_fossil_types.groupby(group, dropna=False).agg(
            {'שווי': 'sum'})
    ).reset_index()
    summary = summary.merge(summary_non_fossil_types,
                            left_on=group,
                            right_on=group,
                            how='left',
                            suffixes=['', ' בסוגי החזקות לא פוסיליים']
                            )
    summary["שיעור פוסילי מתוך מניות ואגח סחירים + סוגי החזקות לא פוסיליים"] = (
            1.00 * summary["שווי פוסילי במניות ואגח קונצרני סחירים"] /
            (summary["שווי במניות ואגח קונצרני סחירים"] + summary["שווי בסוגי החזקות לא פוסיליים"])
    )
    return summary


def filter_major_companies(holdings):
    """filter holdings to include only major institutions

    :param holdings: DataFrame
    :return: holdings filtered to include only major institutions
    """
    mask = holdings["ParentCorpName"].str.startswith(tuple(get_major_institutions_list()))

    # removing הלמן-אלדובי חח"י גמל בע"מ 515447035 & 520027715, מנורה מבטחים והסתדרות המהנדסים ניהול קופות גמל בע"מ
    filtered = holdings[mask][
        ~holdings[mask]["ParentCorpLegalId"].isin(['I_520027715', 'I_515447035'])
    ]

    filtered['ParentCorpGroup'] = filtered['ParentCorpName'].str.split().str[0].str.split("-").str[0]
    filtered['ReportPeriodDate'] = filtered['ReportPeriodDesc'].map(report_period_desc_to_date)
    return filtered


def group_fossil_holdings_quarters_institutions(holdings, quarters, institutions):
    """group fossil holdings for given quarters and institutions, to reflect held companies

    :param holdings: holdings DataFrame
    :param quarters: quarters, e.g. '2020 רבעון 1'
    :param institutions: institution short name (first word)
    :return: fossil holdings for the given quarters and institutions, grouped to reflect held companies
    """
    # filter holdings
    if "ParentCorpGroup" not in holdings.columns:
        holdings['ParentCorpGroup'] = holdings['ParentCorpName'].str.split().str[0].str.split("-").str[0]
    fossil_holdings = holdings[
        (holdings["holding_type"].isin(['מניות', 'אג"ח קונצרני'])) &
        (holdings["is_fossil"] == 1) &
        (holdings["ReportPeriodDesc"].isin(quarters)) &
        (holdings["ParentCorpGroup"].isin(institutions))
        ]
    # 1. Handle Israeli holdings
    # 1a. group by Israeli security number
    il_fossil_holdings = fossil_holdings.groupby(
        ["ParentCorpGroup", "ReportPeriodDate", 'מספר ני"ע'])
    il_fossil_holdings_agg = il_fossil_holdings.agg(
        name=pd.NamedAgg(column="שם המנפיק/שם נייר ערך", aggfunc="first"),
        issuer_num=pd.NamedAgg(column="מספר מנפיק", aggfunc="first"),
        il_corp_num=pd.NamedAgg(column="מספר תאגיד", aggfunc="first"),
        fossil_sum=pd.NamedAgg(column="שווי פוסילי", aggfunc="sum"),
        quantity_sum=pd.NamedAgg(column="ערך נקוב", aggfunc="sum")
    ).reset_index()
    # use il_corp_num when issuer_num when missing
    il_fossil_holdings_agg["issuer_num"] = il_fossil_holdings_agg["issuer_num"].fillna(
        il_fossil_holdings_agg["il_corp_num"])
    # use name when issuer_num and il_corp_num are missing
    il_fossil_holdings_agg["issuer_num"] = il_fossil_holdings_agg["issuer_num"].fillna(
        il_fossil_holdings_agg['מספר ני"ע'])
    # 1b. group by Israeli issuer number
    il_fossil_holdings_by_issuer = il_fossil_holdings_agg.groupby(["ParentCorpGroup", "ReportPeriodDate", 'issuer_num'])
    il_fossil_holdings_by_issuer_agg = il_fossil_holdings_by_issuer.agg(
        name=pd.NamedAgg(column="name", aggfunc="first"),
        fossil_sum=pd.NamedAgg(column="fossil_sum", aggfunc="sum"),
        quantity_sum=pd.NamedAgg(column="quantity_sum", aggfunc="sum")
    ).reset_index()
    print("total Israeli fossil holdings: {}".format(il_fossil_holdings_by_issuer_agg["fossil_sum"].sum()))
    print("total fossil holdings with il_sec_num: {}".format(
        fossil_holdings[fossil_holdings['מספר ני"ע'].notnull()]["שווי פוסילי"].sum()
    ))

   # 2. handle non-Israeli holdings
    non_il_fossil_holdings = fossil_holdings[fossil_holdings['מספר ני"ע'].isnull()]
    non_il_missing_ISIN_cnt = non_il_fossil_holdings["ISIN"].isnull().sum()
    if non_il_missing_ISIN_cnt > 0:
        print("there are {} fossil holdings without Israeli sec num and ISIN".format(non_il_missing_ISIN_cnt))
    print("total fossil holdings without il_sec_num: {}".format(non_il_fossil_holdings["שווי פוסילי"].sum()))
    # 2a. group by ISIN
    non_il_fossil_holdings = non_il_fossil_holdings.groupby([
        "ParentCorpGroup", "ReportPeriodDate", 'ISIN'
    ])
    non_il_fossil_holdings_agg = non_il_fossil_holdings.agg(
        name=pd.NamedAgg(column="שם המנפיק/שם נייר ערך", aggfunc="first"),
        issuer_num=pd.NamedAgg(column="מספר מנפיק", aggfunc="first"),
        il_corp_num=pd.NamedAgg(column="מספר תאגיד", aggfunc="first"),
        lei=pd.NamedAgg(column="LEI", aggfunc="first"),
        fossil_sum=pd.NamedAgg(column="שווי פוסילי", aggfunc="sum"),
        quantity_sum=pd.NamedAgg(column="ערך נקוב", aggfunc="sum")
    ).reset_index()

    print(non_il_fossil_holdings_agg["fossil_sum"].sum())
    # Fill in issuer_num for missing LEIs, then il_corp_num, then ISIN
    non_il_fossil_holdings_agg["issuer_num"] = non_il_fossil_holdings_agg["issuer_num"].fillna(
        non_il_fossil_holdings_agg["lei"])
    non_il_fossil_holdings_agg["issuer_num"] = non_il_fossil_holdings_agg["issuer_num"].fillna(
        non_il_fossil_holdings_agg["il_corp_num"])
    non_il_fossil_holdings_agg["issuer_num"] = non_il_fossil_holdings_agg["issuer_num"].fillna(
        non_il_fossil_holdings_agg["ISIN"])
    non_il_fossil_holdings_by_issuer = non_il_fossil_holdings_agg.groupby([
        "ParentCorpGroup", "ReportPeriodDate", 'issuer_num'
    ], dropna=False)
    non_il_fossil_holdings_by_issuer_agg = non_il_fossil_holdings_by_issuer.agg(
        name=pd.NamedAgg(column="name", aggfunc="first"),
        fossil_sum=pd.NamedAgg(column="fossil_sum", aggfunc="sum"),
        quantity_sum=pd.NamedAgg(column="quantity_sum", aggfunc="sum")
    ).reset_index()
    # add Israeli and non-Israeli holdings
    fossil_holdings_by_issuer_agg = pd.concat([
        il_fossil_holdings_by_issuer_agg, non_il_fossil_holdings_by_issuer_agg])
    # clean holding name
    fossil_holdings_by_issuer_agg["clean_name"] = fossil_holdings_by_issuer_agg["name"].apply(clean_company)
    print("after adding Israeli and non-Israeli: {}".format(fossil_holdings_by_issuer_agg["fossil_sum"].sum()))
    # group by issuer_num
    fossil_holdings_by_issue_grp = fossil_holdings_by_issuer_agg.groupby([
        "ParentCorpGroup", "ReportPeriodDate", 'issuer_num'
    ], dropna=False)
    fossil_holdings_by_issuer_agg = fossil_holdings_by_issue_grp.agg(
        name=pd.NamedAgg(column="clean_name", aggfunc="first"),
        fossil_sum=pd.NamedAgg(column="fossil_sum", aggfunc="sum"),
        quantity_sum=pd.NamedAgg(column="quantity_sum", aggfunc="sum")
    ).reset_index()
    # group by holding name (clean)
    fossil_holdings_by_issue_grp = fossil_holdings_by_issuer_agg.groupby([
        "ParentCorpGroup", "ReportPeriodDate", 'name'
    ], dropna=False)
    fossil_holdings_by_issuer_agg = fossil_holdings_by_issue_grp.agg(
        id=pd.NamedAgg(column="issuer_num", aggfunc="first"),
        fossil_sum=pd.NamedAgg(column="fossil_sum", aggfunc="sum"),
        quantity_sum=pd.NamedAgg(column="quantity_sum", aggfunc="sum")
    ).reset_index()
    return fossil_holdings_by_issuer_agg


def compare_fossil_holdings_over_quarters(holdings, quarters, institution):
    fossil_holdings_grouped = group_fossil_holdings_quarters_institutions(holdings, quarters, institution)
    # divide to quarters (take only first 2)
    report_periods = np.sort(fossil_holdings_grouped["ReportPeriodDate"].unique())
    prev_q = fossil_holdings_grouped[fossil_holdings_grouped["ReportPeriodDate"] == report_periods[0]]
    curr_q = fossil_holdings_grouped[fossil_holdings_grouped["ReportPeriodDate"] == report_periods[1]]
    # join quarters to compare fossil holdings one by one
    cols = ["name", "id", "fossil_sum", "quantity_sum"]
    comparison = prev_q[cols].merge(curr_q[cols],
                                    on="id",
                                    how="outer",
                                    suffixes=["_prev_q", "_curr_q"]
                                    )
    comparison["name_prev_q"] = comparison["name_prev_q"].fillna(comparison["name_curr_q"])
    comparison = comparison.drop("name_curr_q", axis=1).rename({"name_prev_q": "name"}, axis=1)
    comparison = comparison.fillna(0).sort_values("name")
    comparison["quantity_diff"] = comparison["quantity_sum_curr_q"] - comparison["quantity_sum_prev_q"]
    comparison["quantity_diff_pct"] = 1.00 * comparison["quantity_diff"] / comparison["quantity_sum_prev_q"]
    comparison["fossil_sum_diff"] = comparison["fossil_sum_curr_q"] - comparison["fossil_sum_prev_q"]
    comparison["fossil_sum_diff_pct"] = 1.00 * comparison["fossil_sum_diff"] / comparison["fossil_sum_prev_q"]
    return comparison
