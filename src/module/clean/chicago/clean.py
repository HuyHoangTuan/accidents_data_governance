import pandas as pd
import numpy  as np
from constants import CONST


def process_crash_date(table):
    def process_crash_day_of_week():
        day_of_week = CONST['Chicago']['DAY_OF_WEEK']

        def get_day(value):
            for key in day_of_week:
                if value == day_of_week[key]:
                    return key
            return None

        for (index, row) in enumerate(table['CRASH_DAY_OF_WEEK']):
            table.at[index, 'CRASH_DAY_OF_WEEK'] = get_day(row)

    process_crash_day_of_week()
    
    return table


def process_alignment(table):
    for (index, row) in enumerate(table['ALIGNMENT']):
        split_row = ()
        row = row.split(', ')
        for element in row:
            element = element.split('AND')
            for e in element:
                split_row += (e.lstrip().rstrip(),)
        table.at[index, 'ALIGNMENT'] = split_row
    table['ALIGNMENT'] = table['ALIGNMENT'].apply(lambda x: str(x))
    return table

def process_lighting_condition(table):
    for (index, row) in enumerate(table['LIGHTING_CONDITION']):
        split_row = []
        row = row.split(',')
        for element in row:
            split_row.append(element.lstrip().rstrip())
        table.at[index, 'LIGHTING_CONDITION'] = split_row
    table['LIGHTING_CONDITION'] = table['LIGHTING_CONDITION'].apply(lambda x: str(x))
    return table

def process_road_defect(table):
    for (index, row) in enumerate(table['ROAD_DEFECT']):
        if row == 'UNKNOWN': 
            table.at[index, 'ROAD_DEFECT'] = 'NO DEFECTS'
    # print(table['ROAD_DEFECT'].unique())
    return table

def process_sec_contributory_cause(table):
    for (index, row) in enumerate(table['SEC_CONTRIBUTORY_CAUSE']):
        split_row = []
        row = row.split('/')
        for element in row:
            split_row.append(element.lstrip().rstrip())
        table.at[index, 'SEC_CONTRIBUTORY_CAUSE'] = split_row
    # print(table['SEC_CONTRIBUTORY_CAUSE'])
    return table

def process_crash_type(table):
    for (index, row) in enumerate(table['CRASH_TYPE']):
        split_row = []
        row = row.split('AND / OR')
        for element in row:
            element = element.split('/')
            for e in element:
                split_row.append(e.lstrip().rstrip())
        table.at[index, 'CRASH_TYPE'] = split_row    
    return table

def process_work_zone(table):
    table['WORK_ZONE_I'] = table['WORK_ZONE_I'].fillna('N',)
    table['WORK_ZONE_TYPE'] = table['WORK_ZONE_TYPE'].fillna('UNKNOWN')
    table['WORKERS_PRESENT_I'] = table['WORKERS_PRESENT_I'].fillna('N')
    for (index, row) in enumerate(table['WORK_ZONE_I']):
        if row == 'Y':
            table.at[index, 'WORK_ZONE_I'] = (table.at[index, 'WORK_ZONE_I'], \
                                            table.at[index, 'WORK_ZONE_TYPE'], \
                                            table.at[index, 'WORKERS_PRESENT_I'])
        else:
            table.at[index, 'WORK_ZONE_I'] = ('N',)
    table['WORK_ZONE_I'] = table['WORK_ZONE_I'].apply(lambda x: str(x))
    return table.drop(columns=['WORK_ZONE_TYPE', 'WORKERS_PRESENT_I'])

def process_lane_cnt(table):
    return table

def process_intersection_related(table):
    table['INTERSECTION_RELATED_I'] = table['INTERSECTION_RELATED_I'].fillna('N')
    table['INTERSECTION_RELATED_I'] = pd.factorize(table['INTERSECTION_RELATED_I'])[0]
    return table

def process_not_right_of_way(table):
    table['NOT_RIGHT_OF_WAY_I'] = table['NOT_RIGHT_OF_WAY_I'].fillna('N')
    return table

def process_street_direction(table):
    table['STREET_DIRECTION'] = table['STREET_DIRECTION'].fillna('NOT DETERMINABLE')
    return table

def process_injuries_total(table):
    table['INJURIES_TOTAL'] =   table['INJURIES_INCAPACITATING'] + \
                                table['INJURIES_NON_INCAPACITATING'] + \
                                table['INJURIES_REPORTED_NOT_EVIDENT'] + \
                                table['INJURIES_NO_INDICATION'] + \
                                table['INJURIES_UNKNOWN'] + \
                                table['INJURIES_FATAL'] 
    return table

def process_num_units(table):
    table['NUM_UNITS'] = table['NUM_UNITS'].fillna(0)
    table['NUM_UNITS'] = table['NUM_UNITS'].apply(int)
    return table

def process_injuries_fatal(table):
    table['INJURIES_FATAL'] = table['INJURIES_FATAL'].fillna(0)
    table['INJURIES_FATAL'] = table['INJURIES_FATAL'].apply(int)
    return table

def process_injuries_incapacitating(table):
    table['INJURIES_INCAPACITATING'] = table['INJURIES_INCAPACITATING'].fillna(0)
    table['INJURIES_INCAPACITATING'] = table['INJURIES_INCAPACITATING'].apply(int)
    return table

def process_injuries_non_incapacitating(table):
    table['INJURIES_NON_INCAPACITATING'] = table['INJURIES_NON_INCAPACITATING'].fillna(0)
    table['INJURIES_NON_INCAPACITATING'] = table['INJURIES_NON_INCAPACITATING'].apply(int)
    return table

def process_injuries_reported_not_evident(table):
    table['INJURIES_REPORTED_NOT_EVIDENT'] = table['INJURIES_REPORTED_NOT_EVIDENT'].fillna(0)
    table['INJURIES_REPORTED_NOT_EVIDENT'] = table['INJURIES_REPORTED_NOT_EVIDENT'].apply(int)
    return table

def process_injuries_no_indication(table):
    table['INJURIES_NO_INDICATION'] = table['INJURIES_NO_INDICATION'].fillna(0)
    table['INJURIES_NO_INDICATION'] = table['INJURIES_NO_INDICATION'].apply(int)
    return table

def process_injuries_unknown(table):
    table['INJURIES_UNKNOWN'] = table['INJURIES_UNKNOWN'].fillna(0)
    table['INJURIES_UNKNOWN'] = table['INJURIES_UNKNOWN'].apply(int)
    return table

def clean_data(table):
    table = process_crash_date(table)
    table = process_alignment(table)
    table = process_lighting_condition(table)
    table = process_road_defect(table)
    # table = process_sec_contributory_cause(table)
    table = process_work_zone(table)
    table = process_lane_cnt(table)
    table = process_intersection_related(table)
    table = process_not_right_of_way(table)
    table = process_street_direction(table)
    table = process_injuries_fatal(table)
    table = process_injuries_incapacitating(table)
    table = process_injuries_non_incapacitating(table)
    table = process_num_units(table)
    """
    
    # check for any empty values
    for column in table:
        try:
            print(column)
            print(table[column].unique())
        except:
            pass
    """
    # print(table)
