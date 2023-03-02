import pandas as pd

from kw.helper.misc import extract_schema_columns

class Transformer:
  separator = '->'
  underscore = '_'

  def __init__(self, data_dicts: dict) -> None:
    self.data_dicts = data_dicts
    self.df = pd.json_normalize(data_dicts, sep=self.separator)
    self.df['key_request_id'] = self.df['_id']

  def frame_obj(self, record_path: list) -> pd.DataFrame:
    prefix_regex = '^{}{}'.format(self.separator.join(record_path), self.separator)
    filter_regex = '{}|{}'.format('^key_request_id$', prefix_regex)

    df = self.df.filter(regex=filter_regex)
    df.columns = df.columns.str.replace(prefix_regex, '')
    df.columns = df.columns.str.replace(Transformer.separator, Transformer.underscore)

    return df

  def frame_nested_list(
      self, df: pd.DataFrame, key: str,
      additional_keys: list=None, schema: str='',
  ) -> pd.DataFrame:
    columns = ['key_request_id'] if additional_keys == None else additional_keys
    columns.append(key)

    df_obj_list = df.reindex(columns=columns).explode(key)

    if df_obj_list.dropna().empty:
      return df_obj_list.dropna()

    df_obj_list[key].fillna({i: {} for i in df_obj_list.index}, inplace=True)
    df_obj_list.reset_index(drop=True, inplace=True)
    df_obj_list = df_obj_list.join(pd.json_normalize(df_obj_list[key], sep=self.separator)).drop(columns=[key])
    df_obj_list.columns = df_obj_list.columns.str.replace(Transformer.separator, Transformer.underscore)

    if not schema:
      return df_obj_list

    schema_columns = extract_schema_columns(schema)

    df_obj_list = df_obj_list.reindex(columns=schema_columns)
    if 'key_request_id' in schema_columns:
      schema_columns.remove('key_request_id')

    df_obj_list.dropna(subset=schema_columns, how='all', inplace=True)

    return df_obj_list
