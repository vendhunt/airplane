
def main(params):
  import pandas as pd
  import requests 
  file = params['lead_file']['url']
  df = pd.read_csv(file)
  print(df)

  
