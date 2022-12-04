def get_leads(db_campaign_id):
	# fucnction that queries the database for leads given a db/airtable campaign_id and returns a list of dictionaries with the data ready for mailstand
    q = f'''
    Select el.lead_id , el.email, el.business_name, el.phone, el.category, el.address, el.city, el.state, el.zip
    From searches s
    left join email_leads el
        on s.search_id  = el.search_id
    left join emails e
        on e.lead_id  = el.lead_id
    -- we are only interested in the campaign we are looking at now
    where s.campaign_id  = '{db_campaign_id}'
    -- we don't want to include any leads that we have sent an email to
    and e.lead_id is null
    -- we only want to import leads that haven't been imported yet.
    and el.mailstand_id is null
    '''
    lead_result = query(q)
    leads = []
    for l in lead_result:
    	l = lead_id, email, business_name, phone, category, address, city, state
		data = {
		'owner_key': ms_api,
		'workspace': workspace,
		'upload_options': {
			'duplicates': 'skip',
			'strict_validation': False,
			},
		'email': email,
		'company_name': business_name,
		'company_phone': phone, table
		'company_industry': category,
		'address': address,
		'city': city,
		'state': state,
		'zip': zipc,
		'custom_1': lead_id
		}
		leads.append(data)

	return leads


def send_to_mailstand(lead, ms_campaign):
  # function that sends a lead to the mailstand api
  ms_api = os.environ.get('mailstand_key')
  workspace = 'space_RKk16IjgZXHnSVTtrbiEpw2fx'

	headers = {
	'Content-type': 'application/json',
	'Accept': 'application/json'
	}
  # the campaign needs to be added to the lead
	lead['upload_options']['campaign'] = ms_campaign
  # mostly for reporting
	lead_id = lead['custom1']
  # the lead needs to be converted to json
	d = json.dumps(lead)
	# send to mailstand
	r = requests.post(url=url, data=d, headers=headers, auth=auth)

  if r.status_code in [200, 201]:
      output['new_leads'].append(lead_id)
      # update the database_id with the users campaign id
      try:
          q = f'''
          update email_leads
              set mailstand_id = '{r.json()['id']}'
          where lead_id = lead_id
          '''
          rcount = query(q, qtype='update', row_count=True)

      except Exception as e:
        output['db_failures'].append(lead_id)
          print(f'error updating the database for {lead_id}: {e}')

    else:
        output['api_errors'].append(lead_id)

def get_campaigns():
  # function that returns a list of active campaigns that have mailstand ids 
  from pyairtable import Table
  from pyairtable.formulas import match
  airtable_key = os.environ.get('airtable_key')
  base_id = 'apprnuOZwl0l4Fysm'
  table = Table(airtable_key, base_id, 'Campaigns')
  campaigns = table.all(formula=match({'Status':'Active'}))
  # filter to only campaigns that are already in mailstand
  mailstand_campaigns = [x for x in campaigns if 'Mailstand Campaign ID' in x['fields']]
  return mailstand_campaigns

def main():
	global HTTPBasicAuth
	global query
	global requests
	global time 
	global output

	import requests 
	from requests.auth import HTTPBasicAuth
	import time 
	from vh_utils import query

	output = {
		'new_leads':[],
		'db_failures':[],
		'api_errors':[],
		'other_errors':[]
	}

	camps = get_campaigns()
	for camp in camps:
		leads = get_leads(camp['id'])
		# if there aren't any new leads, skip this campaign. 
		if len(leads) == 0:
			continue 
		for lead in leads:
			lead_id = lead['custom1']
			try:
				send_to_mailstand(lead, camp['Mailstand Campaign ID'])
				output['new_leads'].append(new_leads)
			except Exception as e:
				lead_id = 
				print(f'error for lead {lead_id}: {e}')
				output['other_errors'].append(lead_id)
        
  # create the output summary      
  for key in output:
    if key != 'summary':
        output['summary'][key] = len(output[key])
  return output