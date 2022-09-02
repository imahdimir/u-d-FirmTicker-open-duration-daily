"""

  """

##

import sys

from githubdata import GithubData
from mirutil.df_utils import save_as_prq_wo_index as sprq
import pandas as pd
from mirutil.df_utils import read_data_according_to_type as read_data
from shared import c , ra
from shared import status_simplified


def add_mktopen_to_dst(dst , dfip) :
  dst[c.dt] = pd.to_datetime(dst[c.dt])
  dst[c.d] = dst[c.dt].dt.date

  df1 = dfip[[c.id , c.d , c.sdt , c.edt]]

  for cn in [c.id , c.d] :
    df1[cn] = df1[cn].astype(str)
    dst[cn] = dst[cn].astype(str)

  dst = dst.merge(df1 , on = [c.id , c.d] , how = 'left')

  for cn in [c.dt , c.sdt , c.edt] :
    dst[cn] = pd.to_datetime(dst[cn])

  dst[c.ismktopen] = dst[c.dt].ge(dst[c.sdt])
  dst[c.ismktopen] &= dst[c.dt].le(dst[c.edt])

  _tfu = lambda x : x.any()
  _by = [c.id , c.dt]
  dst[c.ismktopen] = dst.groupby(_by)[c.ismktopen].transform(_tfu)

  dst = dst[[c.id , c.dt , c.ismktopen , c.trdble]]

  dst = dst.drop_duplicates(subset = [c.id , c.dt])

  return dst

def main() :

  pass

  ##
  rp_fip = GithubData(ra.fip)
  dfip = rp_fip.read_data()
  dfip.head()
  ##
  dfip = dfip[[c.id , c.d , c.sdt , c.edt]]
  dfip.head()
  ##
  dfip[c.id] = dfip[c.id].astype(str)
  ##


  ##
  rp_stch = GithubData(ra.stch)
  dst = rp_stch.read_data()
  ##
  dst[c.trdble] = dst[c.ns].map(status_simplified)
  ##
  dst = dst[[c.id , c.dt , c.trdble]]
  dst.head()
  ##
  dst[c.id] = dst[c.id].astype(str)
  ##
  dst = add_mktopen_to_dst(dst , dfip)
  dst.head()
  ##


  ##
  dfip1 = dfip[[c.id]]
  dfip1[[c.dt]] = dfip[[c.sdt]]

  dfip2 = dfip[[c.id]]
  dfip2[[c.dt]] = dfip[[c.edt]]
  ##
  dfip = pd.concat([dfip1 , dfip2])

  del dfip1
  del dfip2
  ##
  dfip[c.ismktopen] = True
  ##
  dfip.head()
  ##

  dfip[c.dt] = dfip[c.dt].astype(str)
  dst[c.dt] = dst[c.dt].astype(str)
  ##
  df = dfip.merge(dst , on = [c.id , c.dt] , how = 'outer')
  df.head()

  ##
  del dfip
  ##

  msk = df.duplicated(subset = [c.id , c.dt] , keep = False)
  msk &= df[c.trdble].isna()

  df1 = df[msk]
  df = df[~ msk]
  ##

  msk = df[c.ismktopen + '_x'].isna()
  print(len(msk[msk]))

  df.loc[msk , c.ismktopen + '_x'] = df[c.ismktopen + '_y']

  ##
  df.drop(columns = c.ismktopen + '_y' , inplace = True)

  df.rename(columns = {
      c.ismktopen + '_x' : c.ismktopen
      } , inplace = True)
  ##
  df.head()
  ##

  df = df.sort_values(by = [c.id , c.dt])
  df.head()
  ##
  df[c.trdble] = df.groupby([c.id])[c.trdble].ffill()
  ##
  msk = df[c.ismktopen]
  msk &= df[c.trdble].notna()

  df1 = df[~ msk]

  df = df[msk]
  ##
  df.head()
  ##


  ##
  df.drop(columns = c.ismktopen , inplace = True)
  ##
  df[c.d] = pd.to_datetime(df[c.dt]).dt.date

  ##
  df[c.ndt] = df.groupby([c.id , c.d])[c.dt].shift(-1)
  df.head()

  ##
  _col = pd.to_datetime(df[c.ndt]) - pd.to_datetime(df[c.dt])
  _col = _col.dt.seconds
  ##
  msk = df[c.trdble]

  df.loc[msk , c.dur] = _col[msk]

  df.head()
  ##
  df1 = df.groupby([c.id , c.d])[c.dur].sum()
  df1.head()
  ##
  df1 = df1.to_frame()
  df1.reset_index(inplace = True)

  ##
  df1[c.dur] = df1[c.dur].astype(int)
  ##
  df1.head()

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

##