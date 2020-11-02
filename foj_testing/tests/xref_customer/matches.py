import pandas as pd
from match_status import Match_Status

def matches(row, source_columns, target_columns):
   results = []
   mismatches = []
   
   if row["_merge"] == 'left_only':
      return pd.Series([Match_Status.NO_RECORD_IN_TARGET.value, mismatches])
   elif row["_merge"] == 'right_only':
      return pd.Series([Match_Status.NO_RECORD_IN_SOURCE.value, mismatches])
   else:
      for i in range(len(source_columns)):
         if (isinstance(row[source_columns[i]+'_source'], str) and isinstance(row[target_columns[i]+'_target'], str)):
            if ((row[source_columns[i]+'_source'].strip().upper()) == (row[target_columns[i]+'_target'].strip().upper())):
               results.append(Match_Status.MATCHED.value)
            elif ((pd.isna(row[source_columns[i]+'_source']) and len(row[target_columns[i]+'_target'].strip()) == 0) or
                  (pd.isna(row[target_columns[i]+'_target']) and len(row[source_columns[i]+'_source'].strip()) == 0)):
                     results.append(Match_Status.MATCHED.value)                
            else:
               #print('Mismatched: '+source_columns[i]+ '  '+row[source_columns[i]+'_source']+ ' / '+ row[target_columns[i]+'_target'])
               results.append(Match_Status.RECORDS_NOT_EQUAL.value)
               mismatches.append(source_columns[i])
         else:
            if row[source_columns[i]+'_source'] == row[target_columns[i]+'_target']:
               results.append(Match_Status.MATCHED.value)
            else:
               results.append(Match_Status.RECORDS_NOT_EQUAL.value)
               mismatches.append(source_columns[i])

      if (len(list(set(results))) == 1):
         return pd.Series([list(set(results))[0], mismatches])
      else: 
         return pd.Series([Match_Status.RECORDS_NOT_EQUAL.value, mismatches])
