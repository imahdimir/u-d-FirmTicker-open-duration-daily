"""

  """

##

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