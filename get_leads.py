def main(params):
    import os 
    import time
    from vh_utils import query

    global d7key
    d7key = os.environ.get('d7_key')
    
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
    
    return camp_count
