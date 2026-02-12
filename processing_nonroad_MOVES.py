"""
Pre-processing of MOVES nonroad output data
"""

import yaml
import pandas as pd
import numpy as np
from pathlib import Path
import re


parent_path = Path(__file__).parent
data_path =  parent_path / 'data'

## Read in MOVES data

groupby_cols = ['yearid',
                'fuel',
                'scc',
                'equipment',
                'pollutant',
                ]

file = '20251202_flcac_nr_emission_factors_all_pollutants'
## Check Teams (Task 3 Transportation Datasets / MOVES) for the latest file
df_orig = pd.read_excel(data_path / f'{file}.xlsx', thousands=',', sheet_name='emission_factors')

with open(data_path / "moves_nonroad_inputs.yaml", "r") as file:
    moves_inputs = yaml.safe_load(file)

tech_flows = (pd.read_csv(data_path / 'moves_nonroad_flows.csv')
              .filter(['equipment', 'flowname', 'FlowContext', 'ProcessContext'])
              .dropna(subset='flowname')
              )
moves_inputs['tech_flows'] = dict(zip(tech_flows['equipment'], tech_flows['flowname']))
moves_inputs['ProcessContext'] = dict(zip(tech_flows['equipment'], tech_flows['ProcessContext']))
moves_inputs['FlowContext'] = dict(zip(tech_flows['equipment'], tech_flows['FlowContext']))

energy_flow = moves_inputs['EnergyFlow']

# identify fuel types, prepared to be included in process name
fuel_map = {
    "LPG": "Liquified Petroleum Fuel",
    "Gasoline": "Gasoline",
    "Nonroad Diesel": "Diesel"
}

## equipment column is unique identifier; scc, sector, and fuel are additional information

#%%
df = (df_orig
      .drop(columns=['EF(g/MJ)'], errors='ignore')
      .drop(columns=['stateid', 'state'], errors='ignore')
      .drop(columns=['fuelsubtypeid', 'fueltypeid', 'pollutantid', 'sectorid'], errors='ignore')
      .groupby(groupby_cols)
      .agg('sum')
      .assign(region = 'US')
      .reset_index()
      .assign(EF = lambda x: x['inv_mass'] / x['energy'])
      .assign(Unit = 'kg')
      .assign(Context = 'air')
      )

#%% Align elementary flows with FEDEFL
from esupy.mapping import apply_flow_mapping
from esupy.util import make_uuid

kwargs = {}
kwargs['material_crosswalk'] = (data_path /
                                'MOVES_nonroad_fedefl_flow_mapping.csv')
## ^^ hack to pass a local mapping file

mapped_df = apply_flow_mapping(
    df=df, source=None, flow_type='ELEMENTARY_FLOW',
    keep_unmapped_rows=True, ignore_source_name=True,
    field_dict = {
        'SourceName': '',
        'FlowableName': 'pollutant',
        'FlowableUnit': 'Unit',
        'FlowableContext': 'Context',
        'FlowableQuantity': 'EF',
        'UUID': 'FlowUUID'},
    **kwargs
    )

df = (mapped_df.reset_index(drop=True)
      .rename(columns={'pollutant': 'FlowName',
                       'EF': 'amount',
                       'Unit': 'unit'})
      )

#%% Update the reference_flow_var for each process
temp_df = (df[['yearid', 'fuel', 'scc', 'equipment', 'sector', 'region']]
           .drop_duplicates()
           )
if 'description' not in df:
    df['description'] = ''
else:
    df['description'] = df['description'].fillna('')

df_olca = pd.concat([df,
                     (temp_df
                      .assign(reference = True)
                      .assign(IsInput = False)
                      .assign(amount = 1)
                      .assign(FlowName = 'reference_flow_var')
                      .assign(description = '')
                      ), ## ^^ reference flow output
                     (temp_df
                      .assign(reference = False)
                      .assign(IsInput = True)
                      .assign(amount = 1)
                      .assign(FlowName = energy_flow)
                      .assign(description = '')
                      ) ## ^^ energy inputs
                     ], ignore_index=True)

# fill the nan values for energycontent, energy, source_hrs, population, load_factor, avg_hp, EF(kg/MJ),EF(kg/hr)
cols_to_fill = [
    "energycontent", "energy", "source_hrs", "population",
    "load_factor", "avg_hp", "EF(kg/MJ)", "EF(kg/hr)"]

df_olca[cols_to_fill] = (
    df_olca.groupby("equipment")[cols_to_fill]
           .transform(lambda x: x.fillna(method="ffill").fillna(method="bfill")))

# Update syntax for transport types
df_olca['name'] = df_olca['equipment'].map(moves_inputs['tech_flows'])
df_olca['ProcessCategory'] = df_olca['equipment'].map(moves_inputs['ProcessContext'])
df_olca['RefFlowCategory'] = df_olca['equipment'].map(moves_inputs['FlowContext'])
# drop unwanted technologies
df_olca = df_olca.dropna(subset=['name'])

cond1 = df_olca['FlowName'] == 'reference_flow_var'
cond2 = df_olca['FlowName'] == energy_flow
df_olca = (df_olca
           .assign(ProcessName = lambda x: ('Operation of equipment; ' + x['name'] + '; '
                                            + x['fuel'].map(fuel_map).str.lower()
                                            + ' powered'))
           .assign(reference = np.where(cond1, True, False))
           .assign(IsInput = np.where(cond2, True, False))
           .assign(FlowType = np.where(cond1 | cond2, 'PRODUCT_FLOW',
                   'ELEMENTARY_FLOW'))
           .assign(unit = np.where(cond1 | cond2, 'MJ', df_olca['unit']))
           .assign(FlowName = lambda x: np.where(cond1,
                   x['ProcessName'].str.rsplit(';', n=1).str.get(0),
                   x['FlowName']))
           .assign(FlowName = lambda x: np.where(cond2, x['fuel'], x['FlowName']))
           ##TODO: ^^ fix this flow name assignment for reference flows
           .assign(FlowUUID = lambda x: np.where(cond1,
                   x['name'].apply(make_uuid), x['FlowUUID']))
           .assign(Context = lambda x: np.where(cond1,
                   'Technosphere Flows / ' + df_olca['RefFlowCategory'],
                   df_olca['Context']))
           .assign(location = lambda x: np.where(
                   x['region'] == 'US', 'US', None))
           .assign(ProcessID = lambda x: x.apply(
               lambda z: make_uuid(z['ProcessName'], z['location']), axis=1))
           .drop(columns=['name'])
           )


#%% Update the fuel_type_var for each process
from flcac_utils.mapping import prepare_tech_flow_mappings

## Identify mappings for technosphere flows (fuel inputs)
fuel_df = pd.read_csv(data_path / 'MOVES_nonroad_fuel_mapping.csv')
fuel_df = fuel_df.query('SourceFlowName in @df_olca.fuel')
fuel_dict, flow_objs, provider_dict = prepare_tech_flow_mappings(fuel_df)

#%% apply mappings
from flcac_utils.mapping import apply_tech_flow_mapping, create_bridge_processes

df_olca = apply_tech_flow_mapping(df_olca.rename(columns={'FlowName':'name'}),
                                  fuel_dict, flow_objs, provider_dict)
df_olca = df_olca.query('not(FlowUUID.isna())')

df_bridge = create_bridge_processes(df_olca, fuel_dict, flow_objs)

# df_olca.to_csv(parent_path /'moves_processed_output.csv', index=False)

from flcac_utils.generate_processes import build_flow_dict
flows, new_flows = build_flow_dict(
    pd.concat([df_olca, df_bridge], ignore_index=True))
# pass bridge processes too to ensure those flows get created
#%%

# import unit converstion file to include hour as an alternative unit for all processes
df_processes = df_olca[df_olca["FlowType"] == "PRODUCT_FLOW"]

altflowlist = pd.DataFrame(columns=["Flowable", "AltUnit", "Unit", "AltUnitConversionFactor", "InverseConversionFactor"])
altflowlist["Flowable"] = df_processes["FlowName"]
altflowlist["AltUnit"] = "h"
altflowlist["Unit"] = "MJ"
altflowlist["AltUnitConversionFactor"] = df_processes["source_hrs"]/df_processes["energy"]*1000
altflowlist["InverseConversionFactor"] = df_processes["energy"]/(df_processes["source_hrs"]*1000)
altflowlist.drop(altflowlist[altflowlist["Flowable"] == "Liquefied petroleum gas, dispensed at pump"].index, inplace=True)
#%%
# # change converstion factor in the alt_unit attribute in the flows dictionary 
# for flow_id, flow in flows.items():
#     altunits = altflowlist[altflowlist['Flowable'] == flow.name]
#     if not hasattr(flow, "alt_units"):
#         flow.alt_units = []
#     for _, alt in altunits.iterrows():
#         alt_unit_info = {
#             "unit": alt["AltUnit"],
#             "conversion_factor": alt["AltUnitConversionFactor"],
#             "inverse_conversion_factor": alt["InverseConversionFactor"]
#         }
#         flow.alt_units.append(alt_unit_info)


# change converstion factor in the flow property attribute in the flows dictionary 
import olca_schema as o
import olca_schema.units as units
import logging as log
for flow_id, flow in flows.items():
    altunits=altflowlist[altflowlist['Flowable']==flow.name]
    for i, alternate in altunits.iterrows():
        altfp = o.FlowPropertyFactor()
        altfp.is_ref_flow_property = False
        altfp.conversion_factor = alternate['AltUnitConversionFactor']
        altfp.flow_property = units.property_ref(alternate["AltUnit"])
        if altfp.flow_property is None:
            log.warning(f"unknown altunit {alternate['AltUnit']} "
                                        f"in flow {flow.name}")
        else:
            flow.flow_properties.append(altfp)
#%%
# #check if alternative units are created
# for uuid, flow in flows.items():
#     if hasattr(flow, "alt_units") and flow.alt_units:
#         print(f"Flow: {flow.name} ")
#         print("alt_units:", flow.alt_units)
        
#check if flow properties are changed
if "d9ce3d3b-eb45-3cf2-a28a-5fea57e16694" in flows:
    flow_obj = flows["d9ce3d3b-eb45-3cf2-a28a-5fea57e16694"]
    flow_properties = getattr(flow_obj, "flow_properties", None)
    print("flow_properties:", flow_properties)

    
#%%
# replace newly created flows with those pulled via API
api_flows = {flow.id: flow for k, flow in flow_objs.items()}
if not(flows.keys() | api_flows.keys()) == flows.keys():
    print('Warning, some flows not consistent')
else:
    flows.update(api_flows)

## Consider LCIA validation?

#%% Assign exchange dqi
from flcac_utils.util import format_dqi_score, increment_dqi_value
df_olca['exchange_dqi'] = format_dqi_score(moves_inputs['DQI']['Flow'])
# drop DQI entry for reference flow
df_olca['exchange_dqi'] = np.where(df_olca['reference'] == True,
                                    '', df_olca['exchange_dqi'])

#%% prepare metadata
from flcac_utils.generate_processes import build_location_dict
from flcac_utils.util import assign_year_to_meta, \
    extract_actors_from_process_meta, extract_dqsystems,\
    extract_sources_from_process_meta, generate_locations_from_exchange_df

with open(data_path / 'MOVES_nonroad_process_metadata.yaml') as f:
    process_meta = yaml.safe_load(f)

process_meta = assign_year_to_meta(process_meta, moves_inputs['Year'])
process_meta['time_description'] = (process_meta['time_description']
                                    .replace('[YEAR]', str(moves_inputs['Year']))
                                    )
(process_meta, source_objs) = extract_sources_from_process_meta(
    process_meta, bib_path = data_path / 'transport.bib')
(process_meta, actor_objs) = extract_actors_from_process_meta(process_meta)
dq_objs = extract_dqsystems(moves_inputs['DQI']['dqSystem'])
process_meta['dq_entry'] = format_dqi_score(moves_inputs['DQI']['Process'])


# prepare locations
locations = generate_locations_from_exchange_df(df_olca)
location_objs = build_location_dict(df_olca, locations)

#%% Build json file
from flcac_utils.generate_processes import \
    build_process_dict, write_objects, validate_exchange_data

validate_exchange_data(df_olca)

processes = {}

# function used to create process names without fuel description at the beginning
def get_equipment_desc(s: str) -> str:
    # Case 1: pattern "XXX - YYYYYY"
    if re.match(r".+ - .+", s):
        return s.split(" - ", 1)[1]   # keep only after the dash
    # Case 2: pattern starts with "2/4-Str"
    elif re.match(r"a-Str\s+.+", s):
        return s                      # keep whole string
    # Default: fallback
    else:
        return s.title()
#df_olca['process_name'] = df_olca['equipment'].apply(get_equipment_desc) #add column that has the process name without fuel description at the beginning
df_olca['process_name'] = df_olca['equipment'].apply(get_equipment_desc) + ", " + df["fuel"].map(fuel_map) + " powered"
    
# loop through each vehicle type and fuel to adjust metadata before writing processes
for s in df_olca['equipment'].unique():
    _df_olca = df_olca.query('equipment == @s')
    avg_hp = _df_olca['avg_hp']
    for i in _df_olca[['region', 'fuel','sector','load_factor','avg_hp']].drop_duplicates().itertuples(index=False):
        _process_meta = process_meta.copy()
        if i.region == 'US':
            _process_meta['geography_description'] = _process_meta.get('geography_description_US')
        _process_meta.pop('geography_description_US')
        
        for k, v in _process_meta.items():
            if not isinstance(v, str): continue
            v = v.replace('[Title]',_df_olca['process_name'].iloc[0])
            v = v.replace('[equipment]', _df_olca['process_name'].iloc[0].lower())
            v = v.replace('[FUEL]', i.fuel.lower())
            v = v.replace ('[LOAD_FACTOR]', format(i.load_factor,".2g"))
            v = v.replace ('[avg_hp]', format(i.avg_hp, ".2g"))
            v = v.replace('[sector]', i.sector.lower())
            _process_meta[k] = v
            
        p_dict = build_process_dict(_df_olca.query('region == @i.region'),
                                    flows, meta=_process_meta,
                                       loc_objs=location_objs,
                                       source_objs=source_objs,
                                       actor_objs=actor_objs,
                                       dq_objs=dq_objs,
                                       )
        processes.update(p_dict)
# build bridge processes
bridge_processes = build_process_dict(df_bridge, flows, meta=moves_inputs['Bridge'])


#%% Write to json
out_path = parent_path / 'output'
write_objects('moves_nonroad', flows, new_flows, processes,
              source_objs, actor_objs, dq_objs, location_objs, bridge_processes,
              out_path = out_path)
## ^^ Import this file into an empty database with units and flow properties only
## or merge into USLCI and overwrite all existing datasets

#%% Unzip files to repo
from flcac_utils.util import extract_latest_zip

extract_latest_zip(out_path,
                   parent_path,
                   output_folder_name = Path('output') / 'moves_nonroad_v1.0')
