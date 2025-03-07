### Federal LCA Commons | Medium- and Heavy-Duty Emission Rates
# Author: Andrew Eilbert, Created: 10/7/2024

import pandas as pd
from _datetime import datetime as dt
import time
import os
from sqlalchemy import create_engine
#import numpy as np
#from sklearn import preprocessing

start_time = dt.now()
print("start time: ", start_time)
datestamp = time.strftime('%Y%m%d')
pd.set_option('display.max_columns', None)

user = 'root'
pw = 'root'
host = 'localhost'
port = 3306

os.chdir("C:/Users/AEilbert/OneDrive - Eastern Research Group/Documents/FLCAC/data/")
ef_file = datestamp + "_flcac_mhd_emission_factors.csv"
output_file = datestamp + "_flcac_mhd_moves_output.xlsx"
default_db = 'movesdb20240104'
run_date = 20240813
output_db = str(run_date) + "_flcac_moves_efs_out"

mariadb_url = "mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, pw, host, port, default_db)
sql_directory = "C:/ProgramData/MariaDB/MariaDB 10.11/data/"
engine = create_engine(mariadb_url)

source_types = {32: 'Light Commercial Trucks', 50: 'Single Unit Trucks', \
                52: 'Single Unit Trucks, Short-Haul', 53: 'Single Unit Trucks, Long-Haul', \
                60: 'Combination Trucks', 61: 'Combination Trucks, Short-Haul', \
                62: 'Combination Trucks, Long-Haul', 70: 'MHD Trucks'}
    
fuel_types = {1: 'Gasoline', 2: 'Diesel', 3: 'Compressed Natural Gas (CNG)', 5: 'E85', 9: 'Electricity'}

payloads = {32: 1.00, 50: 6.94,
            52: 6.94, 53: 6.94,
            60: 16.18, 61: 16.18,
            62: 16.18, 70: 23.12}

inv_query = "SELECT a.yearID,a.stateID,b.stateAbbr,a.sourceTypeID, a.fuelTypeID,\
            a.pollutantID,c.pollutantName,sum(a.emissionQuant) AS 'inventory' \
            FROM `" + output_db + "`.`movesoutput` a \
            JOIN `" + default_db + "`.`state` b \
            JOIN `" + default_db + "`.`pollutant` c \
            ON a.stateID=b.stateID AND a.pollutantID=c.pollutantID \
            WHERE a.sourceTypeID IN (32,52,53,61,62) \
            GROUP BY stateID,sourceTypeID,fuelTypeID,pollutantID,yearID \
            ORDER BY stateID,sourceTypeID,fuelTypeID,pollutantID,yearID;"
            
act_query = "SELECT a.yearID,a.stateID,b.stateAbbr,a.activityTypeID,a.sourceTypeID,\
            a.fuelTypeID,sum(a.activity) AS 'activity' \
            FROM `" + output_db + "`.`movesactivityoutput` a \
            JOIN `" + default_db + "`.`state` b \
            ON a.stateID=b.stateID \
            WHERE a.activityTypeID=1  AND a.sourceTypeID IN (32,52,53,61,62) \
            GROUP BY stateID,activityTypeID,sourceTypeID,fuelTypeID,yearID \
            ORDER BY stateID,activityTypeID,sourceTypeID,fuelTypeID,yearID;"

raw_inv_df = pd.read_sql(inv_query, engine)            
raw_act_df = pd.read_sql(act_query, engine)

with pd.ExcelWriter(output_file) as writer:  
    raw_inv_df.to_excel(writer, sheet_name='inventory', index=False)
    raw_act_df.to_excel(writer, sheet_name='activity', index=False)
    
print(raw_inv_df)
print(raw_act_df)

state_inv_df = pd.DataFrame()
state_act_df = pd.DataFrame()

inv_holds = ['yearID', 'stateAbbr', 'sourceTypeID', 'fuelTypeID', 'pollutantName']
inv_drops = ['stateID', 'pollutantID']

act_holds = ['yearID', 'stateAbbr', 'sourceTypeID', 'fuelTypeID']
act_drops = ['stateID', 'activityTypeID']


def df_region_reorg (raw_df, hold_cols, drop_cols):
    state_df = raw_df.groupby(hold_cols, as_index=False).sum().copy().drop(columns=drop_cols)
    return state_df


state_inv_df = df_region_reorg(raw_inv_df, inv_holds, inv_drops)        
state_act_df = df_region_reorg(raw_act_df, act_holds, act_drops) 

su_inv_df = pd.DataFrame()
su_act_df = pd.DataFrame()
combo_inv_df = pd.DataFrame()
combo_act_df = pd.DataFrame()
mhd_inv_df = pd.DataFrame()
mhd_act_df = pd.DataFrame()


def aggregated_st_results (region_df, veh_types, new_st, cols):
    
    filtered_df = region_df[region_df['sourceTypeID'].isin(veh_types)].copy().assign(sourceTypeID=new_st)
    aggregated_df = filtered_df.groupby(by=cols, as_index=False).sum().copy()
    return aggregated_df


su_inv_df = aggregated_st_results(state_inv_df, [52, 53], 50, inv_holds)
combo_inv_df = aggregated_st_results(state_inv_df, [61, 62], 60, inv_holds) 
mhd_inv_df = aggregated_st_results(state_inv_df, [32, 52, 53, 61, 62], 70, inv_holds)

agg_inv_df = pd.concat([su_inv_df, combo_inv_df, mhd_inv_df], ignore_index=True)

su_act_df = aggregated_st_results(state_act_df, [52, 53], 50, act_holds)
combo_act_df = aggregated_st_results(state_act_df, [61, 62], 60, act_holds) 
mhd_act_df = aggregated_st_results(state_act_df, [52, 53, 61, 62], 70, act_holds)

agg_act_df = pd.concat([su_act_df, combo_act_df, mhd_act_df], ignore_index=True)

agg_state_inv_df = pd.concat([state_inv_df, agg_inv_df], ignore_index=True)
agg_state_act_df = pd.concat([state_act_df, agg_act_df], ignore_index=True)

merged_ef_df = agg_state_inv_df.merge(agg_state_act_df, how='left', \
    on=['yearID', 'stateAbbr', 'sourceTypeID', 'fuelTypeID'])


def col_name_mapping(df, new_col, key, col_dict):
    df[new_col] = df[key].map(col_dict)
    

col_name_mapping(merged_ef_df, 'fuel', 'fuelTypeID', fuel_types)
col_name_mapping(merged_ef_df, 'source_type', 'sourceTypeID', source_types)
col_name_mapping(merged_ef_df, 'payload', 'sourceTypeID', payloads)

final_ef_df = merged_ef_df[['yearID', 'source_type', 'fuel', 'stateAbbr', 'pollutantName', 
                            'payload', 'inventory', 'activity']]\
    .assign(emission_factor=merged_ef_df.inventory/(merged_ef_df.activity*merged_ef_df.payload), 
            ef_units='kg/ton-km', energy_units='MMBTU/ton-km')\
           .rename(columns={'yearID': 'year', 'stateAbbr': 'state', 'pollutantName': 'pollutant'})

final_ef_df.to_csv(ef_file, index=False)
print(final_ef_df)

end_time = dt.now()
print("end time: ", end_time)
print("time elapsed: ", end_time-start_time)
    