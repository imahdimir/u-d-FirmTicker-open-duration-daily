"""

  """

##

from datetime import date
from datetime import datetime as dt
from datetime import time

import pandas as pd
from githubdata import GithubData
from mirutil.df_utils import read_data_according_to_type as read_data


class RepoAddresses :
  stch = 'imahdimir/d-clean-d-firm-status-change'
  twd = 'imahdimir/d-TSE-working-days'
  i2f = 'imahdimir/d-TSETMC_ID-2-FirmTicker'
  fip = 'imahdimir/d-firm-possible-trade-spells'

ra = RepoAddresses()

class ColNames :
  jdt = 'JDateTime'
  jd = 'JDate'
  d = 'Date'
  row = 'Row'
  dt = 'DateTime'
  id = 'TSETMC_ID'
  ns = 'NewStatus'
  iso = 'iso'
  t = 'Time'
  trdble = 'Tradable'
  dup = 'Duplicated'
  ismktopen = 'IsMarketOpen'
  sdt = 'StartDateTime'
  edt = 'EndDateTime'
  ndt = 'NextDateTime'
  dur = 'Duration'

c = ColNames()

class Status :
  tradeable = True
  not_tradeable = False

s = Status()

status_simplified = {
    'مجاز'        : s.tradeable ,
    'مجاز-محفوظ'  : s.not_tradeable ,
    'مجاز-متوقف'  : s.not_tradeable ,
    'ممنوع-متوقف' : s.not_tradeable ,
    'ممنوع'       : s.not_tradeable ,
    'ممنوع-محفوظ' : s.not_tradeable ,
    'مجاز-مسدود'  : s.not_tradeable ,
    'ممنوع-مسدود' : s.not_tradeable ,
    }

market_start_time = time(9 , 0 , 0)
market_end_time = time(12 , 30 , 0)

def _ret_status_change_data() :
  rp_stch = GithubData(ra.stch)
  rp_stch.clone()

  dstp = rp_stch.data_fp
  df = read_data(dstp)

  rp_stch.rmdir()

  df[c.trdble] = df[c.ns].map(status_simplified)

  msk = df[c.trdble].isna()
  df1 = df[msk]
  assert df1.empty

  df = df[[c.id , c.dt , c.trdble]]

  return df

def _ret_working_date_data() :
  rp_twd = GithubData(ra.twd)
  rp_twd.clone()

  dtwfp = rp_twd.data_fp
  df = read_data(dtwfp)

  rp_twd.rmdir()

  df = df.reset_index()
  df = df[[c.d]]

  df = df.astype(str)

  return df

def _ret_tsetmc_id_data() :
  rp_i2f = GithubData(ra.i2f)
  rp_i2f.clone()

  difp = rp_i2f.data_fp
  df = read_data(difp)

  rp_i2f.rmdir()

  df = df[[c.id]]

  df = df.astype(str)

  return df

def _ret_df_of_every_second_in_day(start_time = market_start_time ,
                                   end_time = market_end_time
                                   ) :
  arb_date = date(2018 , 1 , 1)

  sdt = dt.combine(arb_date , start_time)
  edt = dt.combine(arb_date , end_time)

  dti = pd.date_range(start = sdt , end = edt , freq = 's')

  df = pd.DataFrame()
  df[c.t] = dti.time
  df = df.astype(str)
  df[c.ismktopen] = True

  return df


##

##