### Federal LCA Commons | Nonroad Emission Factors (New Mexico Sample)
# Author: Andrew Eilbert, Created: 2/11/2025

import pandas as pd
from _datetime import datetime as dt
import time
import os
from sqlalchemy import create_engine
#import numpy as np


start_time = dt.now()
print("start time: ", start_time)
datestamp = time.strftime('%Y%m%d')
pd.set_option('display.max_columns', None)

os.chdir("C:/Users/AEilbert/OneDrive - Eastern Research Group/Documents/FLCAC/data/")
nr_ef_file = datestamp + "_flcac_nr_emission_factors.csv"
#nr_output_file = datestamp + "_flcac_nonroad_output.xlsx"
default_db = 'movesdb20241112'

run_date = 20241216 # Allison ran on AWS; includes both NR exhaust and evap runs

user = 'root'
pw = 'root'
host = 'localhost'
port = 3306

mariadb_url = "mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, pw, host, port, default_db)
engine = create_engine(mariadb_url)

days_in_month_dict = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 
                      9: 30, 10: 31, 11: 30, 12: 31}

fuel_type_dict = {1: 'Gasoline', 3: 'CNG', 4: 'LPG', 23: 'Nonroad Diesel', 24: 'Marine Diesel'}

pollutant_dict = {1: 'THC', 2: 'CO', 3: 'NOx', 5: 'CH4', 31: 'SO2', 87: 'VOC', 90: 'CO2', 
                  99: 'BSFC', 110: 'PM2.5 Exh', 116: 'PM2.5 BW', 117: 'PM2.5 TW'}

energy_content_query = "SELECT fuelsubtypeid,fueltypeid,energycontent FROM `{}`.`nrfuelsubtype` \
                        WHERE fuelsubtypeid IN (12,23,24,30,40);".format(default_db)
energy_content_df = pd.read_sql(energy_content_query, engine)
                        
sector_query = "SELECT sectorid,description AS 'sector' FROM `{}`.`sector`;".format(default_db)
sector_df = pd.read_sql(sector_query, engine)

scc_query = "SELECT scc,description AS 'equipment' FROM `{}`.`nrscc`;".format(default_db)
scc_df = pd.read_sql(scc_query, engine)


def nr_avg_metrics(met, date, yr, at):
    query = "SELECT scc,avg(activity) AS '{0}' \
             FROM {1}_c35_ctfs_nonroad_{2}_out.movesactivityoutput \
             WHERE activitytypeid={3} AND fuelsubtypeid NOT IN (10) \
             GROUP BY scc;".format(met, date, yr, at)
    df = pd.read_sql(query, engine)
    return df


avg_hp_df = nr_avg_metrics('avg_hp', run_date, 2020, 9)
load_factor_df = nr_avg_metrics('load_factor', run_date, 2020, 12)
metrics_df = avg_hp_df.merge(load_factor_df, how='inner', on='scc')
print(metrics_df)

nr_inv_df = pd.DataFrame()
nr_act_df = pd.DataFrame()
nr_emissions = []
nr_activity = []


def compile_nr_emissions(inv_df, emis_lst, date, yr):
    
    inv_query = "SELECT movesrunid,yearid,monthid,dayid,stateid,pollutantid,\
                 fueltypeid,sectorid,scc,sum(emissionquant) AS 'inventory' \
                 FROM `{0}_c35_ctfs_nonroad_{1}_out`.`movesoutput` \
                 WHERE pollutantid IN (1,2,3,5,31,87,90,99,110) \
                 GROUP BY yearid,monthid,dayid,fueltypeid,pollutantid,sectorid,scc;".format(date, yr)
                
    inv_df = pd.read_sql(inv_query, engine)
    return emis_lst.append(inv_df)


def compile_nr_activity(act_df, act_lst, date, yr):
    
    act_query = "SELECT movesrunid,yearid,monthid,dayid,stateid,activitytypeid,\
                 fueltypeid,sectorid,scc,sum(activity) AS 'activity' \
                 FROM `{0}_c35_ctfs_nonroad_{1}_out`.`movesactivityoutput` \
                 GROUP BY yearid,monthid,dayid,fueltypeid,sectorid,scc,activitytypeid;".format(date, yr)
                
    act_df = pd.read_sql(act_query, engine)
    return act_lst.append(act_df)


compile_nr_emissions(nr_inv_df, nr_emissions, run_date, 2020)    
nr_emissions_df = pd.concat(nr_emissions, ignore_index=True)
nr_emissions_df['weekly_inv'] = nr_emissions_df.inventory * nr_emissions_df.dayid

compile_nr_activity(nr_act_df, nr_activity, run_date, 2020)    
nr_activity_df = pd.concat(nr_activity, ignore_index=True)
nr_activity_df['weekly_act'] = nr_activity_df.activity * nr_activity_df.dayid

base_cols = ['yearid', 'stateid', 'fueltypeid', 'sectorid', 'scc']
inv_cols = base_cols + ['pollutantid']
act_cols = base_cols + ['activitytypeid']
inv_months = inv_cols + ['monthid']
act_months = act_cols + ['monthid'] 


def day_to_year_inv(df):
    
    new_cols = ['yearid', 'monthid', 'stateid', 'fueltypeid', 'pollutantid', 'sectorid', 'scc']
    new_df = df.groupby(new_cols, as_index=False)['weekly_inv'].sum().copy()
    new_df['avg_day_inv'] = new_df.weekly_inv / 7
    new_df['month_days'] = new_df['monthid'].map(days_in_month_dict)
    new_df['fuel'] = new_df['fueltypeid'].map(fuel_type_dict)
    new_df['monthly_inv'] = new_df.avg_day_inv * new_df.month_days
    
    agg_cols = ['yearid', 'stateid', 'fueltypeid', 'fuel', 'pollutantid', 'sectorid', 'scc']
    agg_df = new_df.groupby(agg_cols,as_index=False)['monthly_inv'].sum().copy()
    agg_df.rename(columns={'monthly_inv': 'inv_mass'}, inplace=True)
    return agg_df


def day_to_year_act(df):
    
    new_cols = ['yearid', 'monthid', 'stateid', 'fueltypeid', 'activitytypeid', 'sectorid', 'scc']
    new_df = df.groupby(new_cols, as_index=False)['weekly_act'].sum().copy()
    new_df['avg_day_act'] = new_df.weekly_act / 7
    new_df['month_days'] = new_df['monthid'].map(days_in_month_dict)
    new_df['fuel'] = new_df['fueltypeid'].map(fuel_type_dict)
    new_df['monthly_act'] = new_df.avg_day_act * new_df.month_days
    
    agg_cols = ['yearid', 'stateid', 'fueltypeid', 'fuel', 'activitytypeid', 'sectorid', 'scc']
    agg_df = new_df.groupby(agg_cols,as_index=False)['monthly_act'].sum().copy()
    agg_df.rename(columns={'monthly_act': 'activity'}, inplace=True)
    return agg_df


nr_annual_inv_df = day_to_year_inv(nr_emissions_df)
nr_sector_inv_df = nr_annual_inv_df.merge(sector_df, how='left', on='sectorid')
nr_scc_inv_df = nr_sector_inv_df.merge(scc_df, how='left', on='scc')

nr_annual_act_df = day_to_year_act(nr_activity_df)
nr_sector_act_df = nr_annual_act_df.merge(sector_df, how='left', on='sectorid')
nr_scc_act_df = nr_sector_act_df.merge(scc_df, how='left', on='scc')

nr_annual_hrs_df = nr_scc_act_df[nr_scc_act_df['activitytypeid'] == 2].rename(
    columns={'activity': 'source_hrs'}).drop(columns='activitytypeid').copy()
nr_annual_pop_df = nr_scc_act_df[nr_scc_act_df['activitytypeid'] == 6].rename(
    columns={'activity': 'population'}).drop(columns='activitytypeid').copy()

rates_cols = ['yearid', 'fueltypeid' , 'fuel', 'stateid', 'sectorid', 'sector', 'scc', 'equipment']
nr_annual_metrics_df = nr_annual_hrs_df.merge(nr_annual_pop_df, how='outer', on=rates_cols)

nr_annual_bsfc_df = nr_scc_inv_df[nr_scc_inv_df['pollutantid'] == 99].copy()
nr_annual_energy_df = nr_annual_bsfc_df.merge(energy_content_df, how='left', on='fueltypeid')
nr_annual_energy_df['energy'] = nr_annual_energy_df.inv_mass * nr_annual_energy_df.energycontent
nr_annual_energy_df.drop(columns=['pollutantid', 'inv_mass'], inplace=True)

nr_rates_df = nr_scc_inv_df.merge(nr_annual_energy_df, how='inner', on=rates_cols)
nr_final_rates_df = nr_rates_df.merge(nr_annual_metrics_df, how='left', on=rates_cols)

nr_final_rates_df['EF(g/MJ)'] = nr_final_rates_df.inv_mass / nr_final_rates_df.energy.div(1e3)
nr_final_rates_df['EF(g/hr)'] = nr_final_rates_df.inv_mass / nr_final_rates_df.source_hrs
nr_final_rates_df.drop(nr_final_rates_df[nr_final_rates_df.pollutantid == 99].index, inplace=True)
nr_final_rates_df.insert(2, 'state', 'NM')

nr_ef_metrics_df = nr_final_rates_df.merge(metrics_df, how='left', on='scc')


def pollutant_labelling(df):
    if 'pollutantid' in df.columns:
        df['pollutant'] = df['pollutantid'].map(pollutant_dict)
    
    
pollutant_labelling(nr_ef_metrics_df)

reorder_cols = ['yearid', 'stateid', 'state', 'fueltypeid', 'fuel', 'pollutantid', 'pollutant', 
 'sectorid', 'sector','scc', 'equipment', 'inv_mass', 'fuelsubtypeid', 'energycontent', 
 'energy', 'source_hrs', 'population', 'avg_hp', 'load_factor', 'EF(g/MJ)', 'EF(g/hr)']
nr_ef_metrics_df = nr_ef_metrics_df[reorder_cols].copy()
nr_ef_metrics_df.to_csv(nr_ef_file, index=False)
print(nr_ef_metrics_df)


'''
nonroad_invs_df.to_csv(output_path + nr_emission_redux_file, index=False)
#nr_annual_emit_df.to_csv(output_path + nr_inventories_file, index=False)
ctfs_nr_rates_df.to_csv(output_path + nr_rates_file, index=False)
'''

end_time = dt.now()
print("end time: ", end_time)
print("time elapsed: ", end_time-start_time)