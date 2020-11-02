import unittest
import pandas as pd
import numpy
import time
import logging
import os
import json
import yaml
import sys
import errno
from datetime import datetime

import matches
import source_session
import target_session
from match_status import Match_Status


class Testing(unittest.TestCase):

   def setUp(self):

      def initLog():     
         logger = logging.getLogger(self.local_config['test']['name'])
         logPath='./logs/'
         if not os.path.exists(logPath):
            os.makedirs(logPath) 
         logFilename=self.local_config['test']['name']+'.log'  
         hdlr = logging.FileHandler(logPath+logFilename, mode = 'a')
         formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s','%Y-%m-%d %H:%M:%S')
         hdlr.setFormatter(formatter)
         logger.addHandler(hdlr) 
         logger.setLevel(logging.INFO)
         return logger

	
      pd.set_option('display.max_rows', None)
      pd.set_option('display.max_columns', None)
      pd.set_option('display.width', None)
      pd.set_option('display.max_colwidth', -1)

      try:
         with open('../lib/credentials.yaml', 'r') as ymlfile:
            self.global_config = yaml.load(ymlfile)
      except IOError:
         print('Expecting credetials.yaml in ./lib. Not found. Exiting...')
         sys.exit(errno.ENOENT)
      
      try:
         with open("./config.yaml", 'r') as yml2file:
            self.local_config = yaml.load(yml2file)
      except IOError:
         print('Expecting config.yaml. Not found. Exiting...')
         sys.exit(errno.ENOENT)


      self.start_time = tick = datetime.now()
      self.logger = initLog()
      self.target_session = target_session.target_session(self.global_config['foundry']['proxy'])
      self.source_session = source_session.source_session(self.global_config['teradata']['host'],self.global_config['teradata']['user'],self.global_config['teradata']['password'],self.global_config['teradata']['database'])

   def test_xref_pnr_customer(self):

      def get_source_data():
         return pd.read_sql(self.local_config['source']['query'],self.source_session)
      
      def get_target_data():
         retval = target_session.get_target_data(self.target_session, self.global_config['foundry']['api_token'], self.global_config['foundry']['api_url'], self.local_config['target']['query'])
         return retval
            
      # Load source...
      print('Loading source...')
      source_start_time = datetime.now()
      df_source = get_source_data()
      self.source_load_time = round((datetime.now() - source_start_time).total_seconds(),2)
      print('Source loaded.')      
      
      self.source_size = len(df_source)
      print(df_source.dtypes)
      print('Source loaded. Duration: %fs Size: %i' % (self.source_load_time, len(df_source)))
      # Prepare source
      print('Preparing source...')
      if 'trim' in self.local_config['source']: 
         if self.local_config['source']['trim']:
            for field in self.local_config['source']['trim']:
               print('   Trimming source field '+field+ '...')
               df_source[field] = df_source[field].apply(lambda this_column: this_column.strip())
      if 'to_datetime' in self.local_config['source']: 
         if 'from_datetime_format' in self.local_config['source']:
            for field in self.local_config['source']['to_datetime']:
               print('   Converting source field '+field+ ' to datetime, then to format: '+self.local_config['source']['from_datetime_format'])
               df_source[field] = pd.to_datetime(df_source[field], errors='coerce').dt.strftime(self.local_config['source']['from_datetime_format'])
         else:
            for field in self.local_config['source']['to_datetime']:
               print('   Converting source field '+field+ ' to datetime...')
               df_source[field] = pd.to_datetime(df_source[field], errors='coerce')
      print('Source prepared...............')
      print(df_source.dtypes)
      print(df_source.head())

      # Load target...
      print('Loading target...')
      target_start_time = datetime.now()
      df_target = get_target_data() 
      self.target_load_time = round((datetime.now() - target_start_time).total_seconds(),2)
      self.target_size = len(df_target)
      if self.target_size == 0:
         self.fail('Problem connecting to target...')
      else:
         print(df_target.dtypes)
         print('Target loaded. Duration: %fs Size: %i'  % (self.target_load_time, self.target_size))

      # Prepare target...
      print('Preparing target...')
      if 'to_datetime' in self.local_config['target']:
         if self.local_config['target']['to_datetime']:
            for field in self.local_config['target']['to_datetime']:
               print('   Converting target field '+field+ ' to datetime...')
               df_target[field] = pd.to_datetime(df_target[field], errors='coerce')
      if 'to_integer' in self.local_config['target']:
         for field in self.local_config['target']['to_integer']:
            print('   Converting target field '+field+ ' to integer...')
            df_target[field] = pd.to_numeric(df_target[field], errors='coerce')
      print('Target prepared...............')
      print(df_target.dtypes)
      print(df_target.head())

      # Do foj...
      print('Doing FOJ...')
      foj_start_time = datetime.now()
      df_foj = pd.merge(df_source, df_target, on=self.local_config['source']['join_fields'], how='outer', suffixes=('_source', '_target'), indicator = True)
      self.foj_execution_time = round((datetime.now() - foj_start_time).total_seconds(),2)
      print(df_foj.dtypes)
      print('FOJ Complete. FOJ execution time: %fs' % (self.foj_execution_time))


      # Do validation...
      print('Doing validation...')
      validation_start_time = datetime.now()
      df_foj[['status','mismatched_columns']] =  df_foj.apply(lambda each_row: matches.matches(each_row, self.local_config['source']['match_columns'],self.local_config['target']['match_columns']), axis=1)            
      results = df_foj['status'].value_counts()
      self.validation_execution_time = round((datetime.now() - validation_start_time).total_seconds(),2)
      print('Validation complete. Validation execution time %fs' % (self.validation_execution_time))      

      # Do matches...
      print('Doing matches...')
      df_matches = df_foj.loc[((df_foj['status'] == Match_Status.MATCHED.value)), self.local_config['matches']['output']]        

      df_matches.sort_values(by=self.local_config['source']['join_fields']).head(10000).to_csv('./logs/'+time.strftime("%Y%m%d%H%M%S")+'_'+self.local_config['test']['name']+'_matches'+'.csv', index=False, na_rep='null')      
      print('Matches complete...')

      # Do mismatches...
      print('Doing mismatches...')
      if ((Match_Status.NO_RECORD_IN_SOURCE.value in results) or (Match_Status.NO_RECORD_IN_TARGET.value in results) or (Match_Status.RECORDS_NOT_EQUAL.value in results)):
         df_mismatches = df_foj.loc[((df_foj['status'] == Match_Status.NO_RECORD_IN_TARGET.value) | 
                                     (df_foj['status'] == Match_Status.NO_RECORD_IN_SOURCE.value) | 
                                     (df_foj['status'] == Match_Status.RECORDS_NOT_EQUAL.value)), 
                                       self.local_config['mismatches']['output']]  
       
         df_mismatches.sort_values(by=self.local_config['source']['join_fields']).head(10000).to_csv('./logs/'+time.strftime("%Y%m%d%H%M%S")+'_'+self.local_config['test']['name']+'_mismatches'+'.csv', index=False, na_rep='null') 
      print('Mismatches complete...')
 
      # Doing logs
      print('Doing logs...')
      results_json = json.loads(results.to_json(orient='index'))
      results_json['duration'] = round((datetime.now() - self.start_time).total_seconds(),2)
      results_json['source_load_time'] = self.source_load_time
      results_json['target_load_time'] = self.target_load_time
      results_json['foj_execution_time'] = self.foj_execution_time
      results_json['validation_execution_time'] = self.validation_execution_time
      results_json['source_size'] = self.source_size
      results_json['target_size'] = self.target_size
      self.logger.info(json.dumps(results_json))      
      print('Log complete.')

if __name__ == '__main__':
   unittest.main()
