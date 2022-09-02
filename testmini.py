"""

  """

##

import sys

import pandas as pd
import pyspark.sql.functions as sfunc
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import concat_ws

from shared import _ret_df_of_every_second_in_day
from shared import _ret_status_change_data
from shared import c
from shared import market_end_time
from shared import market_start_time


spark = SparkSession.builder.getOrCreate()

def main() :
  pass

  ##
  dst = _ret_status_change_data()
  ##
  dti = _ret_df_of_every_second_in_day(market_start_time , market_end_time)
  ##
  dtw = pd.DataFrame({
      c.d : ['2015-06-02'] ,
      })
  did = pd.DataFrame({
      c.id : ['70289374539527245']
      })
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
  dst1 = dst[msk]
  dst1 = dst1[[c.id , c.dt , c.trdble]]
  ##
  sds = spark.createDataFrame(dst1)
  ##
  sd1 = sd.join(sds , [c.id , c.dt] , how = 'outer')
  sd = sd1
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
  sd = sd.groupBy([c.id , c.d]).count()
  ##
  sdv = sd.toPandas()
  ##
  msk = sdv[c.ismktopen].isna()
  len(msk[msk])

  ##
  df = pd.merge(did , dtw , how = 'cross')
  df = pd.merge(df , dti , how = 'cross')

  ##
  df[c.dt] = df['Date'] + ' ' + df['Time']

  ##
  df = df[[c.id , c.dt , c.ismktopen]]

  ##
  msk = dst[c.id].eq('70289374539527245')
  msk &= dst[c.dt].str[:10].eq('2015-06-02')
  df1 = dst[msk]
  df1 = df1[[c.dt , c.trdble]]

  ##
  df2 = pd.merge(df , df1 , how = 'outer' , on = [c.dt])
  ##
  df2 = df2[[c.dt , c.ismktopen , c.trdble]]
  df2 = df2.sort_values([c.dt])
  ##
  df2[c.trdble] = df2[c.trdble].ffill()
  ##
  msk = df2[c.trdble].eq(True)
  msk &= df2[c.ismktopen].eq(True)
  df3 = df2[msk]

  ##


  ##

  ##

##


##


if __name__ == '__main__' :
  main()
  print('done')


##