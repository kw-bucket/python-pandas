import os
import textwrap

from datetime import datetime
from kw.service.etl.etl import ETL

def start():
  start_datetime = datetime.combine(datetime.strptime(os.getenv('DATA_STARTED_DATE'), '%Y-%m-%d'), datetime.min.time())
  end_datetime = datetime.combine(datetime.strptime(os.getenv('DATA_ENDED_DATE'), '%Y-%m-%d'), datetime.max.time())

  welcome_message = '''
  .==========.==========.==========.
   START ETL
  .==========.==========.==========.
   From: {0}
   To:   {1}
  .----------.----------.----------.'''.format(start_datetime, end_datetime)

  print(
    textwrap.dedent(welcome_message)
  )

  process(start_datetime, end_datetime)

def process(start_datetime: datetime, end_datetime: datetime):
  etl = ETL(start_datetime, end_datetime, load_bucket='kw')

  etl.application()
  etl.bankruptcy()

  etl.decision()
  etl.decision_input_data_decision_output_data()
  etl.decision_output_data()

  etl.employee()
  etl.existing_loan_accounts()

  etl.financial_profile()
  etl.financial_profile_profile_income_statements_accounts()
  etl.financial_whitelist()

  etl.lending_blacklist()
  etl.ncrs()

  etl.phone_metadata()

  etl.ta_score()
  etl.tdg()
  etl.tmn_score()
  etl.wallet_blacklist()
