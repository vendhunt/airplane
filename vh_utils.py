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
