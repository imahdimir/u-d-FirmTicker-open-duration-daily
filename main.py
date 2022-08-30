"""

  """
##
from githubdata import GithubData
from mirutil.df_utils import read_data_according_to_type as read_data


stch_url = 'https://github.com/imahdimir/d-status-changes'

def main() :
  pass

  ##
  rp_stch = GithubData(stch_url)
  rp_stch.clone()

  ##
  dsfp = rp_stch.data_fp
  ds = read_data(dsfp)

  ##
  ds['JDate'].max()


  ##


  ##

##


##


if __name__ == '__main__' :
  main()
  print('done')


##