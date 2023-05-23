import pandas as pd
from constantce import CONST


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
        split_row = []
        row = row.split(', ')
        for element in row:
            element = element.split('AND')
            for e in element:
                split_row.append(e.lstrip().rstrip())
        table.at[index, 'ALIGNMENT'] = split_row
    return table

def process_lighting_condition(table):
    for (index, row) in enumerate(table['LIGHTING_CONDITION']):
        split_row = []
        row = row.split(',')
        for element in row:
            split_row.append(element.lstrip().rstrip())
        table.at[index, 'LIGHTING_CONDITION'] = split_row
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
    # print(table['CRASH_TYPE'])
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
    return table.drop(columns=['WORK_ZONE_TYPE', 'WORKERS_PRESENT_I'])

def clean_data(table):
    table = process_crash_date(table)
    table = process_alignment(table)
    table = process_lighting_condition(table)
    table = process_road_defect(table)
    # table = process_sec_contributory_cause(table)
    table = process_crash_type(table)
    table = process_work_zone(table)
    # print(table)
