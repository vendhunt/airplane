def query(q, get_row_id = False, qtype = 'select', row_count = False):
    import mysql.connector

    db_user = os.environ.get('db_user')
    db_pass = os.environ.get('db_pass')
    db_host = os.environ.get('db_host')
  
    cnx = mysql.connector.connect(
        host=db_host, user=db_user, passwd=db_pass, database='vhdb', port=3306)
    cursor = cnx.cursor()
    cursor.execute(q)
    if qtype in ['insert','update']:
        cnx.commit()
    elif qtype == 'select':
        result = [x for x in cursor]
        cursor.close()
        return result
    
    if row_count:
        count = cursor.rowcount
        return count

    if get_row_id:
    #if we want to capture the row id (mainly for insert) then we will do that and close the connection
        row_id = cursor.lastrowid
        cursor.close()
        return row_id
    else:
    #if we don't need the row id, just close the connection .
        cnx.commit()
        cursor.close()

def get_leads():
    lead_q = '''
    With base as (
    Select 
        el.*, s.campaign_id , s.city as campaign_city,
        -- doing this to limit the query size to a number lower than we will likely need 
        ROW_NUMBER () over (partition by campaign_id order by rand()) as ranking
    From email_leads el 
    -- need searches to bring in the campaign id 
    left join searches s 
        on el.search_id  = s.search_id 
    -- joining emails to make sure we don't email someone we have emailed before
    -- joining on email instead of lead_id in case there was a duplicate or something
    left join emails e 
        on el.email = e.to_email
    -- filtering to people we haven't emailed before. 
    where e.to_email is null 
    and s.campaign_id != 'TBD'
    )
    Select lead_id , email, campaign_id, campaign_city
    From base where ranking <=50
    -- order by ranking is going to alternate the campaigns naturally 
    order by ranking 

    '''

    lead_result = query(lead_q)
    
    return lead_result

def get_campaigns():
    from pyairtable import Table
    airtable_key = os.environ.get('airtable_key')
    base_id = 'apprnuOZwl0l4Fysm'
    table = Table(airtable_key, base_id, 'Campaigns')
    campaigns = table.all()
    camp_dict = {c['id']:c['fields'] for c in campaigns}
    return camp_dict

def get_accounts():
    #getting all the active gmail accounts
    accounts_q = 'select username, email_address from email_accounts where is_active'
    accounts_result = query(accounts_q)
    accounts = {un:{'address':acc,'sent_count':0} for un, acc in accounts_result}
    return accounts 

def get_unsub_emails():
    #to authenticate, we have to get the key (stored as a string, and convert to a dic)
    string_dict = os.environ.get('gsheet_key')
    import ast
    sheets_key = ast.literal_eval(string_dict)
    #the key has /n in it, which broke the serializer. So I manually replaced those with "newline" although we now have to convert back for it to work
    sheets_key['private_key'] = sheets_key['private_key'].replace('newline','\n')
    gc = gspread.service_account_from_dict(sheets_key)

    ss = gc.open_by_key('1PWukYRJ7VfNa6AejCZT1LTlAS_-MaojBeA2xGUQvKjg')
    ws = ss.get_worksheet(0)
    unsub_emails = [x['Email'] for x in ws.get_all_records()]
    
    return unsub_emails


def main(params):
    global gm
    global os
    
    import send_gmail as gm
    import random
    import os
    from email_templates import main_template

    lead_result = get_leads()
    camp_dict = get_campaigns()
    accounts = get_accounts()
    unsub = [] #get_unsub_emails() commenting out for now
    
    #create a service account session for each of the emails 
    delete_accounts = []
    for acc in accounts:
        try:
            service = gm.setup_credentials(acc)
            assert service is not None
            accounts[acc]['service'] = service

        except Exception as e:
            delete_accounts.append(acc)
            print(f'error for {acc}: {e}')
    #have to do the deletion outside of the iteration (python can handle a changing dict size. )
    for acc in delete_accounts:
        del(accounts[acc])
        
    assert len(accounts) > 0 , 'No accounts with valid creds'
    
    counts = {camp_dict[x]['City']:0 for x in camp_dict}

    for lead_id, to_email, campaign_id, campaign_city in lead_result:
        
        if to_email in unsub:
            continue #this skips sending to the current person
            
        signature_addy = camp_dict[campaign_id].get('Address')
        
        sending_account = random.choice(list(accounts.keys()))
        from_email = accounts[sending_account]['address']
        sending_service = accounts[sending_account]['service']
        
        if signature_addy:
            signature_addy = signature_addy.replace('\n',' ')
            city_only = campaign_city.split(',')[0]
            #to_email = 'alexbreen7@gmail.com'
            subject = f'Howdy from another {city_only} Business!'

            assert lead_id is not None, 'error with lead_id'
            assert from_email is not None, 'error with from_email'
            assert to_email is not None, 'error with to_email'
            assert subject is not None , 'error with subject '

            q = f'''
                insert into emails  (lead_id, from_email, to_email, subject, email_date)
                values ({lead_id},'{from_email}','{to_email}','{subject}',current_timestamp())'''
            email_id = query(q, qtype = 'insert', get_row_id = True)
            
            body = main_template(signature_addy,email_id,to_email,city_only)

            assert body is not None, 'body creation failed'
            

            thread_id, msg_id, msg2 = gm.sendMail(to_email, body, subject,
                                      sending_service, html=True, sender=f"Vending Solutions<{from_email}>")
            
            #print(f'sent email to {to_email}')
            counts[campaign_city] += 1 


            q = f'''
            update emails 
            set 
                thread_id = '{thread_id}',
                msg_id = '{msg2}'
            where email_id = {email_id} '''

            query(q,qtype='update')
            
            accounts[sending_account]['sent_count'] += 1
            if accounts[sending_account]['sent_count'] >= 250:
                del(accounts[sending_account])

            #break 
    return [{'city':k, 'count':counts[k]} for k in counts]
