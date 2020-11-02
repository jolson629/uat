from enum import Enum

class Match_Status(Enum):
     MATCHED = 'Matched'
     NO_RECORD_IN_SOURCE = 'No Record In Source'
     NO_RECORD_IN_TARGET = 'No Record in Target'
     RECORDS_NOT_EQUAL = 'Records Not Equal'
