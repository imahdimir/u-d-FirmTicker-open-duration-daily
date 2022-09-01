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
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
from pyspark.sql import Window


spark = SparkSession.builder.getOrCreate()

class RepoAddresses :
  stch = 'imahdimir/d-clean-d-firm-status-change'
  twd = 'imahdimir/d-TSE-working-days'
  i2f = 'imahdimir/d-TSETMC_ID-2-FirmTicker'

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

def main() :
  pass

  ##
  rp_stch = GithubData(ra.stch)
  rp_stch.clone()
  ##
  dstp = rp_stch.data_fp
  dst = read_data(dstp)
  ##
  dst[c.trdble] = dst[c.ns].map(status_simplified)
  ##
  msk = dst[c.trdble].isna()
  df1 = dst[msk]
  assert df1.empty
  ##
  dst = dst[[c.id , c.dt , c.trdble]]
  ##
  rp_twd = GithubData(ra.twd)
  ##
  rp_twd.clone()
  ##
  dtwfp = rp_twd.data_fp
  dtw = read_data(dtwfp)
  dtw = dtw.reset_index()
  dtw = dtw[[c.d]]
  ##
  rp_i2f = GithubData(ra.i2f)
  ##
  rp_i2f.clone()
  ##
  difp = rp_i2f.data_fp
  did = read_data(difp)
  did = did[[c.id]]
  ##
  arbt_day = date(2018 , 1 , 1)

  strtdt = dt.combine(arbt_day , market_start_time)
  enddt = dt.combine(arbt_day , market_end_time)
  dti = pd.date_range(start = strtdt , end = enddt , freq = 's')

  dti = dti.to_frame()
  dti = dti.reset_index(drop = True)
  dti[c.t] = dti[0].dt.time
  dti[c.t] = dti[c.t].astype(str)
  ##
  dti = dti
  dtw = pd.DataFrame({
      c.d : ['2015-06-02'] ,
      })
  did = pd.DataFrame({
      c.id : ['70289374539527245' , '56574323121551263']
      })
  ##
  dti[c.ismktopen] = True
  ##
  sdt = spark.createDataFrame(dti)
  sdw = spark.createDataFrame(dtw)
  sdi = spark.createDataFrame(did)
  ##
  sd = sdi.crossJoin(sdw)
  sd = sd.crossJoin(sdt)
  ##
  newcol = concat_ws(' ' , sd.Date , sd.Time).alias(c.dt)
  sd = sd.select(c.id , newcol , c.ismktopen)
  ##
  dst['1'] = dst[c.dt].astype(str)
  dst['1'] = dst['1'].str[:10]
  ##
  msk = dst[c.id].astype(str).isin(['70289374539527245' , '56574323121551263'])
  msk &= dst['1'].eq('2015-06-02')
  len(msk[msk])
  ##
  dst = dst[msk]
  dst = dst[[c.id , c.dt , c.trdble]]
  ##
  sds = spark.createDataFrame(dst)
  ##
  sd = sd.join(sds , [c.id , c.dt] , how = 'outer')
  ##
  sd = sd.sort([c.id , c.dt])
  ##
  window = Window.partitionBy(c.id).orderBy(c.dt).rowsBetween(-sys.maxsize , 0)
  filled_column = sfunc.last(sd[c.trdble] , ignorenulls = True).over(window)
  sdf_filled = sd.withColumn('filled' , filled_column)
  ##
  sd = sdf_filled
  ##
  msk = sd['filled'] == True
  msk &= sd[c.ismktopen] == True
  sd = sd.filter(msk)
  ##
  sd = sd.select(c.id , c.dt)
  ##
  sd = sd.withColumn(c.d , sfunc.substring(c.dt , 1 , 10))
  ##
  sdv = sd.toPandas()
  ##
  sd = sd.groupBy([c.id , c.d]).count()
  ##
  sdv = sd.toPandas()
  ##
  msk = sdv[c.ismktopen].isna()
  len(msk[msk])

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