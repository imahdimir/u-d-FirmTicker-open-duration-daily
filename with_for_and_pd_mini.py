"""

  """

##

import sys

from datetime import date
from datetime import datetime as dt
from datetime import time

import pandas as pd
import pyspark.sql.functions as sfunc
from githubdata import GithubData
from mirutil.df_utils import read_data_according_to_type as read_data

from shared import _ret_df_of_every_second_in_day
from shared import _ret_status_change_data
from shared import c
from shared import market_end_time
from shared import market_start_time
from shared import _ret_working_date_data
from shared import _ret_tsetmc_id_data


ev_day_seconds_approx = 13000
max_df_row = 20 * 10 ** 6  # 20 M
max_days_for_each_id = max_df_row // ev_day_seconds_approx

def _add_status_chandes(idf , idfs , tsetmc_id) :
  msk = idfs[c.id].eq(tsetmc_id)
  _df = idfs[msk]

  idf = pd.concat([idf , _df])

  return idf

def main() :

  pass

  ##
  dst = _ret_status_change_data()
  dtw = _ret_working_date_data()
  did = _ret_tsetmc_id_data()
  ##
  dst[c.d] = dst[c.dt].str[:10]
  dst[c.t] = dst[c.dt].str[11 :]

  dst1 = dst.drop(columns = [c.dt])
  ##
  dti = _ret_df_of_every_second_in_day(market_start_time , market_end_time)
  ##
  dtt = pd.merge(dtw , dti , how = 'cross')
  dtt = dtt.astype(str)
  ##
  id0 = '26824673819862694'
  ##
  msk = dst1[c.id].eq(id0)
  df1 = dst1[msk]
  len(msk[msk])
  ##
  dtt1 = pd.concat([dtt , df1[[c.d , c.t , c.trdble]]])
  ##
  msk = dtt1.duplicated(subset = [c.d , c.t] , keep = False)
  msk &= dtt1[c.ismktopen].eq(True)
  dtt1 = dtt1[~ msk]
  ##
  dtt1 = dtt1.sort_values([c.d , c.t])
  dtt1v = dtt1.iloc[69 * 10 ** 6 : 69 * 10 ** 6 + 1000]
  ##
  dtt1[c.trdble] = dtt1[c.trdble].ffill()
  ##
  dtt1v = dtt1.iloc[69 * 10 ** 6 : 69 * 10 ** 6 + 1000]

  ##

  ##
  msk = dtt1[c.d].eq('2017-08-20')
  msk &= dtt1[c.t].eq('10:55:42')
  df2 = dtt1[msk]

  ##
  dtt1 = _add_status_chandes(dtt1 , dst , id0)
  ##
  msk = dtt1.duplicated(subset = [c.d , c.t])
  len(msk[msk])
  ##


  ##
  dttv = dtt.head()

  ##


  ##


  ##


  ##


  ##


  ##


  ##


  ##

##


##


if __name__ == '__main__' :
  main()
  print('done')


##