import pandas as pd
from constantce import CONST

def process_crash_data(table):
    def process_crash_day_of_week():
        day_of_week = CONST['Chicago']['DAY_OF_WEEK']
        
        def get_day(value):
            for key in day_of_week:
                if value in day_of_week[key]:
                    return key
            return None
        
        for row in table:
            row = get_day(row)
            pass
        
def clean_data(table):
    process_crash_data(table)    
    print(table)