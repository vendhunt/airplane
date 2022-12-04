def main(params):
  import json
  import os
  from pyairtable import Table
  from pyairtable.formulas import match
  import requests
  from requests.auth import HTTPBasicAuth

  
  #creasting output set
  output = {
      'summary':{},
      'campaign_errors':[],
      'campaign_sucess':[],
      'lead_sucess':[],
      'lead_db_errors':[]
  }
  
  #importing env vars
  airtable_key = os.environ.get('airtable_key')
  base_id = 'apprnuOZwl0l4Fysm'
  ms_api = os.environ.get('mailstand_key')
  workspace = 'space_RKk16IjgZXHnSVTtrbiEpw2fx'
  auth = HTTPBasicAuth(ms_api,"") 
  
  table = Table(airtable_key, base_id, 'Campaigns')
  campaigns = table.all(formula=match({'Status':'Active'}))

  errors = []
  sucess = []
  for c in campaigns[:15]:
      try:
          #if the campaign already has a mailstand ID skip it. 
          ms_cid = c['fields'].get('Mailstand Campaign ID')
          if ms_cid and len(ms_cid) >1 :
              continue

          body = {
              'workspace':workspace,
              'name':c['fields']['Name']
          }
          
          headers={
              'Content-type':'application/json', 
              'Accept':'application/json'
          }
          url = 'https://api.mailstand.com/campaigns'
          d = json.dumps(body)
          r = requests.post(url=url,data=d,headers = headers,auth=auth)

          #get the campaign id and update it in airtable
          ms_cid = r.json()['id']
          table.update(c['id'],{'Mailstand Campaign ID':ms_cid})
          sucess.append(c['id'])
          time.sleep(5)
      except Exception as e:
          errors.append(c['id'])
          print(f'''error for campaign {c['id']}: {e}''')

  output['campaign_errors'].extend(errors)
  output['campaign_sucess'].extend(sucess)

  return output
