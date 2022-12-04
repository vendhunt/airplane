def main(params):
    from vh_utils import query 
    email_sync = '''
    Insert into email_leads 
    With to_import as (
    Select *, ROW_NUMBER() over (PARTITION BY email order by lead_id) as duplicate_email
    From leads 
    where email is not null 
    and email not in (select email from email_leads el)
    ) 
    Select lead_id, search_id, business_name, phone, email, category, address, city, state, zip 
    from to_import 
    where duplicate_email =1 '''


    email_count = query(email_sync, row_count=True, qtype='insert')
    
    phone_sync = '''
    Insert into  phone_leads
    With to_import as (
    Select *, ROW_NUMBER() over (PARTITION BY phone order by lead_id) as duplicate_phone
    From leads 
    where phone is not null 
    and phone not in (select phone from phone_leads)
    ) 
    Select lead_id, search_id, business_name, phone, email, category, address, city, state, zip 
    from to_import 
    where duplicate_phone =1 
    '''

    phone_count = query(phone_sync, row_count=True, qtype='insert')   
    
    return {
        'phone contacts imported':phone_count,
        'email contacts imported':email_count
    }
    
