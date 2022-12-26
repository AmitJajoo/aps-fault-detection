import os,sys

def error_message_details(error,error_details:sys):
    _,_,exc_tb = error_details.exc_tb()
    file_name  = exc_tb.tb_frame.f_code.co_filename
    error_message="Error occured in python script name [{0}] line number [{1}] error message [{2}] ".format(file_name,exc_tb.lineno,str(error))

    return error_message

class SensorException(Exception):
    def __init__(self,error_message,error_details:sys):
        self.error_message = error_message_details(error_message, error_details = error_details)
    
    def __str__(self):
        return self.error_message