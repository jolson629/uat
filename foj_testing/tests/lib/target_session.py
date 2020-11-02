from pypac import PACSession, get_pac
import pandas as pd
import json


def target_session(proxy): 
   pac = get_pac(url=proxy, allowed_content_types=['text/plain'])
   return PACSession(pac)

def get_target_data(session, token, url, query):

   # Use the above to build the headers of the API POST call:
   headers = {
       'Authorization': 'Bearer ' + token,
       'Content-Type': 'application/json'
   }

   # Finally, the API POST call:
   r = session.post(url, data=json.dumps({'query': query}), headers=headers, timeout=1400)

   # Get the response:
   r = r.json()

   # Peel off the column names from the response
   if 'fSchema' in r:
      column_names = [field['name'] for field in r['fSchema']['fieldSchemaList']]
      # Load the response into a data frame, using the column names
      return pd.DataFrame(r['rows'], columns=column_names)
   else:
      print(json.dumps(r))
      return pd.DataFrame()
