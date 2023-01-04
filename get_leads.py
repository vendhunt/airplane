def run_search(kw, city,camp_id):
    #call D7 API to generate search
    url = f'https://dash.d7leadfinder.com/app/api/search/?keyword={kw}&country=US&location={city}&key={d7key}'
    response = requests.get(url)
    #receive JSON with the D7 search ID and the wait time
    search = response.json()
    if 'error' in search:
        raise Exception('D7 Erorr: '+ search['error'])

    d7_id = search['searchid']

    
    now = datetime.now()
    q = f'''insert into searches (city, term, d7_id, search_date, campaign_id) 
    values('{city}', '{kw}', '{d7_id}', current_timestamp(),'{camp_id}')'''
    db_searchid = query(q, get_row_id=True, qtype = 'insert')

    #add the database search id and the time that we can pull the search data into the dictionary
    search['db_searchid'] = db_searchid 
    search['wait_time'] = datetime.now() + timedelta(seconds = int(search['wait_seconds']))
    
    #return the dictionary
    return search 
    
def get_search(d7_searchid, db_searchid):
    url = f'https://dash.d7leadfinder.com/app/api/results/?id={d7_searchid}&key={d7key}'
    results = requests.get(url)
    df = pd.DataFrame(results.json())
    df = df[['name','phone','email','category','address1','address2','region','zip']]
    df.rename(columns = {
        'name':'business_name',
        'address1':'address',
        'address2':'city',
        'region':'state',
    },inplace = True)

    df['search_id'] = db_searchid
    

    from sqlalchemy import create_engine
    db_user = os.environ.get('db_user')
    db_pass = os.environ.get('db_pass')
    db_host = os.environ.get('db_host')
    con_string = new_string = 'mysql+pymysql://{db_user}:{db_pass}@{db_host}:3306/vhdb'.format(
        db_user=db_user,
        db_pass=db_pass,
        db_host=db_host)
    engine = create_engine(con_string)

    df.to_sql('leads', con=engine,
              if_exists='append', index=False)
    
    output = {
        'db_searchid': db_searchid, 
        'length':len(df)
    }
    return output

#for each city, run the search for the top 10 terms
def search_city(camp):
    #pull a list of terms that haven't been searched yet
    q = f'''
    Select t.term
    From terms t
    left join searches s 
        on t.term  = s.term 
        and s.campaign_id  = 'f{camp['id']}'
    where search_id  is null 
    '''
    terms = query(q)

    #generate a search for each term and save the details to a list. 
    searches = []
    for kw in terms:
        search_details = run_search(kw[0],camp['fields']['City'],camp['id'])
        searches.append(search_details)
        time.sleep(10)

    #update airtable with all the terms that have been searched. 
    term_str = '\n'.join([x[0] for x in terms])
    #if there are already terms in the list, let's add those to the beg of term_str
    current_terms = camp['fields'].get('Terms Searched','')
    term_str = current_terms + term_str
    #update the record in airtable. 
    camp_table.update(camp['id'],{'Terms Searched':term_str})
    
    outputs = []
    #retreive the data from the searches. 
    for s in sorted(searches, key=lambda d: d['wait_time']):
        #check to make sure we have waited past the wait time
        if datetime.now() <= s['wait_time']:
            #if it is before the wait time, calculate the seconds to wait and wait that long
            time_to_wait = (s['wait_time'] - datetime.now() ).seconds + 1 
            time.sleep(time_to_wait)

        output = get_search(s['searchid'],s['db_searchid'])
        outputs.append(output)
        time.sleep(10)
        
    for o in outputs:
        o['campaign'] = camp['id']
        o['city']  = camp['fields']['City']
    
    return outputs

def select_campaigns():
    campaigns = camp_table.all(formula=match({'Status':'Active'}))

    #query to find out how many remaining leads each campaign has (and convert to dict so searchable)
    q = '''
    -- figure out which campaigns have the least unemailed leads. 
    select s.campaign_id , count(*) as cnt
    From email_leads el
    left join  searches s on s.search_id  = el.search_id 
    left join emails e  on e.lead_id  = el.lead_id 
    where e.email_id  is null 
    group by 1
    '''
    camp_count = query(q)
    camp_count_dic = {c:cnt for c,cnt in camp_count}

    #add the count of leads left to each campaign. 
    campaigns2 = []
    for c in campaigns:
        c['leads_left'] = camp_count_dic.get(c['id'],0)
        campaigns2.append(c)

    #sort by the number of leads left and select the first 10 campaigns. 
    selected_camps = sorted(campaigns2, key=lambda d: d['leads_left'])[:3]
    
    return selected_camps

def main(params):
    global camp_table
    global datetime
    global d7key
    global os 
    global time 
    global timedelta
    global Table
    global match
    global pd 
    global requests
    global query
    
    from datetime import datetime, timedelta
    import os 
    import pandas as pd 
    from pyairtable import Table
    from pyairtable.formulas import match
    import requests
    from vh_utils import query
    import time  
    
    airtable_key = os.environ.get('airtable_key')
    base_id = 'apprnuOZwl0l4Fysm'
    camp_table = Table(airtable_key, base_id, 'Campaigns')    
    
    d7key = os.environ.get('d7key')

    
    camps = select_campaigns()
    
    results = []
    for camp in camps:
        try:
            r = search_city(camp)
            results.append(r)
        except Exception as e:
            print(f'error for camp:{camp}: {e}')
    
    return results
