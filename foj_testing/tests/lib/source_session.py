import sql

def source_session(host, user, password, database):      
   return sql.connect(None, host=host, user=user, password=password, logmech = 'LDAP', database = database)
