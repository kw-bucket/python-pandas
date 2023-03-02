import pandas as pd
import hashlib
import json

from datetime import datetime
from kw.helper.misc import extract_schema_columns
from kw.service.etl.extractor import Extractor
from kw.service.etl.loader import Loader
from kw.service.etl.transformer import Transformer

class ETL:
  datetime_fmt = '%Y-%m-%dT%H:%M:%S.%f'

  def __init__(self, start_datetime: datetime, end_datetime: datetime, load_bucket: str) -> None:
    extractor = Extractor()
    data_dicts = extractor.extract_data_dicts(start_datetime, end_datetime)

    self.transformer = Transformer(data_dicts)
    self.loader = Loader(bucket=load_bucket, as_of_datetime=start_datetime)

  def application(self) -> None:
    _df = self.transformer.frame_obj(record_path=['decision_input_data', 'application'])

    # application
    #.----------.----------.----------.----------.----------.
    app_table_name = 'application'
    app_schema = 'schema/application/application_schema.json'
    app_schema_cols = extract_schema_columns(app_schema)
    app_df = _df.reindex(columns=app_schema_cols)

    self.__hash(app_df, columns=[
      'personal_info_national_thai_id',
      'personal_info_first_name_en', 'personal_info_last_name_en',
      'personal_info_first_name_th', 'personal_info_last_name_th',
      'personal_info_mobile_number', 'personal_info_contact_number',
      'personal_info_email', 'work_address_office_phone_no'
    ])
    self.__force_int(app_df, columns=['occupation_info_occupation_id'])

    ## application_consent_list
    #.----------.----------.----------.----------.----------.
    consent_list_table_name = 'application_consent_list'
    consent_list_schema = 'schema/application/application_consent_list_schema.json'
    consent_list_df = self.transformer.frame_nested_list(
      df=_df,
      key='consent_list',
      schema=consent_list_schema,
    )

    ## application_questionnaire_list
    #.----------.----------.----------.----------.----------.
    qtn_list_table_name = 'application_questionnaire_list'
    qtn_list_schema = 'schema/application/application_questionnaire_list_schema.json'
    qtn_list_df = self.transformer.frame_nested_list(
      df=_df,
      key='questionnaire_list',
      schema=qtn_list_schema,
    )

    self.__force_int(qtn_list_df, columns=['quiz_id', 'answer_id'])

    ## application_financial_institution_list
    #.----------.----------.----------.----------.----------.
    fin_inst_list_table_name = 'application_financial_institution_list'
    fin_inst_list_schema = 'schema/application/application_financial_institution_list_schema.json'
    fin_inst_list_df = self.transformer.frame_nested_list(
      df=_df,
      key='financial_institution_list',
      schema=fin_inst_list_schema,
    )

    self.__force_int(fin_inst_list_df, columns=['_id'])

    self.loader.load(app_df, app_schema, app_table_name)
    self.loader.load(consent_list_df, consent_list_schema, consent_list_table_name)
    self.loader.load(fin_inst_list_df, fin_inst_list_schema, fin_inst_list_table_name)
    self.loader.load(qtn_list_df, qtn_list_schema, qtn_list_table_name)
    self.__print_hr()

  # bankruptcy
  #.----------.----------.----------.----------.----------.
  def bankruptcy(self) -> None:
    table_name = 'bankruptcy'
    schema = 'schema/bankruptcy/bankruptcy_schema.json'
    schema_cols = extract_schema_columns(schema)
    df = self.transformer.frame_obj(record_path=['decision_input_data', 'bankruptcy']).reindex(columns=schema_cols)

    self.loader.load(df, schema, table_name)
    self.__print_hr()

  # decision
  #.----------.----------.----------.----------.----------.
  def decision(self) -> None:
    table_name = 'decision'
    schema='schema/decision/decision_schema.json'
    schema_cols = extract_schema_columns(schema)

    df = self.transformer.df.reindex(columns=schema_cols)
    df.request_time = pd.to_datetime(df.request_time).dt.strftime(self.datetime_fmt)
    df.decision_time = pd.to_datetime(df.decision_time).dt.strftime(self.datetime_fmt)

    self.loader.load(df, schema, table_name)
    self.__print_hr()

  #.decision_input_data_decision_output_data
  #.----------.----------.----------.----------.----------.
  def decision_input_data_decision_output_data(self) -> None:
    table_name = 'decision_input_data_decision_output_data'
    schema = 'schema/decision_input_data/decision_input_data_decision_output_data_schema.json'
    schema_cols = extract_schema_columns(schema)
    df = self.transformer.frame_obj(record_path=['decision_input_data', 'decision_output_data']).reindex(
      columns=schema_cols,
    )

    self.__force_int(df, columns=['AGE_WHEN_APPLY', 'TRUE_RELATION_MONTH', 'WHITELIST_FLAG', 'TOTAL_BAD_BILL'])

    self.loader.load(df, schema, table_name)
    self.__print_hr()

  # decision_output_data
  #.----------.----------.----------.----------.----------.
  def decision_output_data(self) -> None:
    table_name = 'decision_output_data'
    schema = 'schema/decision_output_data/decision_output_data_schema.json'
    schema_cols = extract_schema_columns(schema)
    df = self.transformer.frame_obj(record_path=['decision_output_data']).reindex(
      columns=schema_cols,
    )
    df.DATETIME = pd.to_datetime(df.DATETIME) \
      .dt.tz_localize('Asia/Bangkok') \
      .dt.tz_convert('UTC') \
      .dt.strftime(self.datetime_fmt)

    self.__force_int(df, columns=['AGE', 'AGE_WHEN_APPLY', 'TRUE_RELATION_MONTH', 'WHITELIST_FLAG', 'TOTAL_BAD_BILL'])

    self.loader.load(df, schema, table_name)
    self.__print_hr()

  # employee
  #.----------.----------.----------.----------.----------.
  def employee(self) -> None:
    table_name = 'employee'
    schema = 'schema/employee/employee_schema.json'
    schema_cols = extract_schema_columns(schema)
    df = self.transformer.frame_obj(record_path=['decision_input_data', 'employee']).reindex(
      columns=schema_cols,
    )

    self.loader.load(df, schema, table_name)
    self.__print_hr()

  def existing_loan_accounts(self) -> None:
    _df = self.transformer.frame_obj(record_path=['decision_input_data', 'existing_loan_accounts'])

    # existing_loan_accounts
    #.----------.----------.----------.----------.----------.
    exg_loan_accs_table_name = 'existing_loan_accounts'
    exg_loan_accs_schema = 'schema/existing_loan_accounts/existing_loan_accounts_schema.json'
    exg_loan_accs_schema_cols = extract_schema_columns(exg_loan_accs_schema)
    exg_loan_accs_df = _df.reindex(columns=exg_loan_accs_schema_cols)

    # existing_loan_accounts_accounts
    #.----------.----------.----------.----------.----------.
    accs_list_table_name = 'existing_loan_accounts_accounts'
    accs_list_schema = 'schema/existing_loan_accounts/existing_loan_accounts_accounts_schema.json'
    accs_list_df = self.transformer.frame_nested_list(
      df=_df,
      key='accounts',
      schema=accs_list_schema,
    )

    self.__force_int(accs_list_df, columns=['dpd'])

    self.loader.load(exg_loan_accs_df, exg_loan_accs_schema, exg_loan_accs_table_name)
    self.loader.load(accs_list_df, accs_list_schema, accs_list_table_name)
    self.__print_hr()

  # financial_profile
  #.----------.----------.----------.----------.----------.
  def financial_profile(self) -> None:
    table_name = 'financial_profile'
    schema = 'schema/financial_profile/financial_profile_schema.json'
    schema_cols = extract_schema_columns(schema)

    df = self.transformer.frame_obj(record_path=['decision_input_data', 'financial_profile']).reindex(
      columns=schema_cols,
    )
    self.loader.load(df, schema, table_name)
    self.__print_hr()

  def financial_profile_profile_income_statements_accounts(self) -> None:
    _df_stmt = self.transformer.frame_obj(
      record_path=['decision_input_data', 'financial_profile', 'profile', 'income', 'statement']
    )
    _df_stmt_accts = self.transformer.frame_nested_list(df=_df_stmt, key='accounts')
    _df_stmt_accts['key_id'] = _df_stmt_accts['key_request_id'] + '_' + _df_stmt_accts.index.astype(str)

    ## financial_profile_profile_income_statement_accounts
    #.----------.----------.----------.----------.----------.
    accts_table_name = 'financial_profile_profile_income_statement_accounts'
    accts_schema = 'schema/financial_profile/financial_profile_profile_income_statement_accounts_schema.json'
    accts_schema_columns = extract_schema_columns(accts_schema)
    accts_df = _df_stmt_accts.reindex(columns=accts_schema_columns)

    _df_stmt_accts.rename(columns={'key_id': 'key_parent_id'}, inplace=True)

    ## financial_profile_profile_income_statement_accounts_invoice
    #.----------.----------.----------.----------.----------.
    invoices_table_name = 'financial_profile_profile_income_statement_accounts_invoice'
    invoices_schema = 'schema/financial_profile/financial_profile_profile_income_statement_accounts_invoice_schema.json'
    invoices_df = self.transformer.frame_nested_list(
      df=_df_stmt_accts,
      schema=invoices_schema,
      key='invoice',
      additional_keys=['key_parent_id'],
    )

    ## financial_profile_profile_income_statement_accounts_subscriber
    #.----------.----------.----------.----------.----------.
    subscribers_table_name = 'financial_profile_profile_income_statement_accounts_subscriber'
    subscribers_schema = 'schema/financial_profile/financial_profile_profile_income_statement_accounts_subscriber_schema.json'
    subscribers_df = self.transformer.frame_nested_list(
      df=_df_stmt_accts,
      schema=subscribers_schema,
      key='subscriber',
      additional_keys=['key_parent_id'],
    )

    self.loader.load(accts_df, accts_schema, accts_table_name)
    self.loader.load(invoices_df, invoices_schema, invoices_table_name)
    self.loader.load(subscribers_df, subscribers_schema, subscribers_table_name)
    self.__print_hr()

  # financial_whitelist
  #.----------.----------.----------.----------.----------.
  def financial_whitelist(self) -> None:
    table_name = 'financial_whitelist'
    schema = 'schema/financial_whitelist/financial_whitelist_schema.json'
    schema_cols = extract_schema_columns(schema)
    df = self.transformer.frame_obj(
      record_path=['decision_input_data', 'financial_whitelist'],
    ).reindex(
      columns=schema_cols,
    )

    self.__hash(df, columns=['mobile', 'thai_id'])

    self.loader.load(df, schema, table_name)
    self.__print_hr()

  def lending_blacklist(self) -> None:
    _df = self.transformer.frame_obj(record_path=['decision_input_data', 'lending_blacklist'])

    ## lending_blacklist
    bl_table_name = 'lending_blacklist'
    bl_schema = 'schema/lending_blacklist/lending_blacklist_schema.json'
    bl_schema_cols = extract_schema_columns(bl_schema)
    bl_df = _df.reindex(columns=bl_schema_cols)

    ## lending_blacklist_blacklist
    bl_bl_table_name = 'lending_blacklist_blacklist'
    bl_bl_schema = 'schema/lending_blacklist/lending_blacklist_blacklist_schema.json'
    bl_bl_df = self.transformer.frame_nested_list(df=_df, key='blacklist')

    self.__hash(bl_bl_df, columns=['_id'])
    self.__force_int(bl_bl_df, columns=['status_severity', 'mobile_creditLimit'])

    self.loader.load(bl_df, bl_schema, bl_table_name)
    self.loader.load(bl_bl_df, bl_bl_schema, bl_bl_table_name)
    self.__print_hr()

  # ncrs
  #.----------.----------.----------.----------.----------.
  def ncrs(self) -> None:
    table_name = 'ncrs'
    schema = 'schema/ncrs/ncrs_schema.json'
    schema_cols = extract_schema_columns(schema)
    df = self.transformer.frame_obj(record_path=['decision_input_data', 'ncrs']).reindex(columns=schema_cols)

    self.loader.load(df, schema, table_name)
    self.__print_hr()

  # phone_metadata
  #.----------.----------.----------.----------.----------.
  def phone_metadata(self) -> None:
    table_name = 'phone_metadata'
    schema = 'schema/phone_metadata/phone_metadata_schema.json'
    schema_cols = extract_schema_columns(schema)

    df = pd.DataFrame(self.transformer.data_dicts)
    df['key_request_id'] = df['_id']
    df['phone_metadata'] = df['decision_input_data'].map(lambda i: json.dumps(i.get('phone_metadata')))
    df = df[['key_request_id', 'phone_metadata']].reindex(columns=schema_cols)

    self.loader.load(df, schema, table_name)
    self.__print_hr()

  def ta_score(self) -> None:
    _df = self.transformer.frame_obj(record_path=['decision_input_data', 'true_analytics_score'])

    # ta_score
    #.----------.----------.----------.----------.----------.
    ta_score_table_name = 'true_analytics_score'
    ta_score_schema = 'schema/ta_score/ta_score_schema.json'
    ta_score_schema_cols = extract_schema_columns(ta_score_schema)
    ta_score_df = _df.reindex(columns=ta_score_schema_cols)

    self.__force_int(ta_score_df, columns=['total_product'])

    ## ta_score_result
    #.----------.----------.----------.----------.----------.
    ta_score_results_table_name = 'true_analytics_score_result'
    ta_score_results_schema = 'schema/ta_score/ta_score_result_schema.json'
    ta_score_results_df = self.transformer.frame_nested_list(
      df=_df,
      key='results',
      schema=ta_score_results_schema,
    )

    self.__force_int(ta_score_results_df, columns=[
      'scores_score_100',
      'score_variables_max_delay_bill_amt_3_mth_all_1_day_delay_stats',
      'score_variables_max_dpd_6_mth_all_1_day_delay_stats',
      'score_variables_cust_aging',
      'score_variables_delay_status_9_mth_all_30_day_delay_stats'
    ])

    self.loader.load(ta_score_df, ta_score_schema, ta_score_table_name)
    self.loader.load(ta_score_results_df, ta_score_results_schema, ta_score_results_table_name)
    self.__print_hr()

  def tdg(self) -> None:
    _df = self.transformer.frame_obj(record_path=['decision_input_data', 'tdg'])
    _df_results = self.transformer.frame_nested_list(df=_df, key='results')
    _df_results['key_id'] = _df_results['key_request_id'] + '_' + _df_results.index.astype(str)

    # tdg
    #.----------.----------.----------.----------.----------.
    tdg_table_name = 'tdg'
    tdg_schema = 'schema/tdg/tdg_schema.json'
    tdg_schema_cols = extract_schema_columns(tdg_schema)
    tdg_df = _df.reindex(columns=tdg_schema_cols)

    ## tdg_results
    #.----------.----------.----------.----------.----------.
    results_table_name = 'tdg_results'
    results_schema = 'schema/tdg/tdg_results_schema.json'
    results_schema_cols = extract_schema_columns(results_schema)
    results_df = _df_results.reindex(columns=results_schema_cols)

    self.__force_int(results_df, columns=['total_score'])

    ## tdg_results_product_scores
    #.----------.----------.----------.----------.----------.
    _df_results.rename(columns={'key_id': 'key_parent_id'}, inplace=True)
    product_scores_table_name = 'tdg_results_product_scores'
    product_scores_schema = 'schema/tdg/tdg_results_product_scores_schema.json'
    product_scores_df = self.transformer.frame_nested_list(
      df=_df_results,
      key='product_scores',
      additional_keys=['key_parent_id'],
      schema=product_scores_schema,
    )

    self.__force_int(product_scores_df, columns=['scores_score_100'])

    self.loader.load(tdg_df, tdg_schema, tdg_table_name)
    self.loader.load(results_df, results_schema, results_table_name)
    self.loader.load(product_scores_df, product_scores_schema, product_scores_table_name)
    self.__print_hr()

  # tmn_score
  #.----------.----------.----------.----------.----------.
  def tmn_score(self) -> None:
    table_name = 'tmn_score'
    schema = 'schema/tmn_score/tmn_score_schema.json'
    schema_cols = extract_schema_columns(schema)

    df = self.transformer.frame_obj(
      record_path=['decision_input_data', 'tmn_score'],
    ).reindex(
      columns=schema_cols,
    )

    self.loader.load(df, schema, table_name)
    self.__print_hr()

  def wallet_blacklist(self) -> None:
    _df = self.transformer.frame_obj(record_path=['decision_input_data', 'wallet_blacklist'])

    ## wallet_blacklist
    #.----------.----------.----------.----------.----------.
    wbl_table_name = 'wallet_blacklist'
    wbl_schema = 'schema/wallet_blacklist/wallet_blacklist_schema.json'
    wbl_schema_cols = extract_schema_columns(wbl_schema)
    wbl_df = _df.reindex(columns=wbl_schema_cols)

    ## wallet_blacklist_wallet_blacklist
    #.----------.----------.----------.----------.----------.
    wbl_wbl_table_name = 'wallet_blacklist_wallet_blacklist'
    wbl_wbl_schema = 'schema/wallet_blacklist/wallet_blacklist_wallet_blacklist_schema.json'
    wbl_wbl_df = self.transformer.frame_nested_list(df=_df, key='wallet_blacklist')

    self.loader.load(wbl_df, wbl_schema, wbl_table_name)
    self.loader.load(wbl_wbl_df, wbl_wbl_schema, wbl_wbl_table_name)
    self.__print_hr()

  def __force_int(self, df: pd.DataFrame, columns: list) -> None:
    if not df.empty:
      df[columns] = df[columns].astype(pd.Int64Dtype())

  def __hash(self, df: pd.DataFrame, columns: list) -> None:
    if not df.empty:
      df[columns] = df[columns].applymap(self.__sha256)

  def __sha256(self, s) -> str:
    return hashlib.sha256(str(s).encode('utf-8')).hexdigest() if not pd.isna(s) else ''

  def __print_hr(self) -> None:
    print('.----------.----------.----------.')
