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

file = '20250211_flcac_nr_emission_factors'
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

## equipment column is unique identifier; scc, sector, and fuel are additional information

#%%
df = (df_orig
      .drop(columns=['load_factor', 'EF(g/MJ)', 'EF(g/hr)'], errors='ignore')
      .drop(columns=['stateid', 'state'])
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
                      .assign(Isinput = True)
                      .assign(amount = 1)
                      .assign(FlowName = energy_flow)
                      .assign(description = '')
                      ) ## ^^ energy inputs
                     ], ignore_index=True)
# Update syntax for transport types
df_olca['name'] = df_olca['equipment'].map(moves_inputs['tech_flows'])
df_olca['ProcessCategory'] = df_olca['equipment'].map(moves_inputs['ProcessContext'])
df_olca['RefFlowCategory'] = df_olca['equipment'].map(moves_inputs['FlowContext'])

cond1 = df_olca['FlowName'] == 'reference_flow_var'
cond2 = df_olca['FlowName'] == energy_flow
df_olca = (df_olca
           .assign(ProcessName = lambda x: ('Operation of equipment, ' + x['name'] + ', '
                                            + x['fuel'].str.lower()
                                            + ' powered, ' + x['region']))
           .assign(reference = np.where(cond1, True, False))
           .assign(IsInput = np.where(cond2, True, False))
           .assign(FlowType = np.where(cond1 | cond2, 'PRODUCT_FLOW',
                   'ELEMENTARY_FLOW'))
           .assign(unit = np.where(cond1 | cond2, 'MJ', df_olca['unit']))
           .assign(FlowName = lambda x: np.where(cond1,
                   x['ProcessName'].str.rsplit(',', n=1).str.get(0),
                   x['FlowName']))
           ##TODO: ^^ fix this flow name assignment for reference flows
           .assign(FlowUUID = lambda x: np.where(cond1,
                   x['name'].apply(make_uuid), x['FlowUUID']))
           .assign(Context = lambda x: np.where(cond1,
                   'Technosphere Flows / ' + df_olca['RefFlowCategory'],
                   df_olca['Context']))
           )


#%% Update the fuel_type_var for each process
from flcac_utils.util import extract_flows

## Identify mappings for technosphere flows (fuel inputs)
with open(data_path / "fuel_mapping.yaml", "r") as file:
    fuel_dict = yaml.safe_load(file)['pumped_fuels']

## extract fuel objects in fuel_dict from commons via API
flow_dict = {}
for k, v in fuel_dict.items():
    if 'repo' in v:
        repo = list(v.get('repo').keys())[0]
        flow = list(v.get('repo').values())[0]
        fuel_dict[k]['target_name'] = flow
        if not fuel_dict[k].get('BRIDGE'):
            fuel_dict[k]['name'] = flow
        if repo in flow_dict:
            flow_dict[repo].extend([flow])
        else:
            flow_dict[repo] = [flow]

flow_dict = extract_flows(flow_dict, add_tags=True)

for k, v in fuel_dict.items():
    if not fuel_dict[k].get('BRIDGE'):
        fuel_dict[k]['id'] = flow_dict.get(v['name']).id
    else:
        fuel_dict[k]['id'] = make_uuid(fuel_dict[k].get('name'))

df_olca = (df_olca
           .assign(bridge = lambda x: np.where(
               cond2, x['fuel'].map({k: True for k, v in fuel_dict.items()
                                     if v.get('BRIDGE', False)}),
               False))
           ## TODO: make sure electricity inputs are flagged for eventual default provider
           .assign(FlowName = lambda x: np.where(
               cond2, x['fuel'].map({k: v['name'] for k, v in fuel_dict.items()}),
               x['FlowName']))
           .assign(FlowUUID = lambda x: np.where(
               cond2, x['fuel'].map({k: v['id'] for k, v in fuel_dict.items()}),
               x['FlowUUID']))
           # ^ only assign UUIDs where a bridge will not be used
           .assign(Context = lambda x: np.where(
               cond2, x['fuel'].map({k: flow_dict.get(v['target_name']).category
                                     for k, v in fuel_dict.items()}),
               x['Context']))
           .assign(location = lambda x: np.where(
               x['region'] == 'US', 'US', None))
           .assign(ProcessName = lambda x: np.where(
               x['region'] == 'US', x['ProcessName'].str.rsplit(',', n=1).str.get(0),
               x['ProcessName']))
           # ^ assign location only for full U.S. process and remove substring
        )

#%% create bridge data
df_bridge = (df_olca[cond2]
             .query('bridge == True')
             .drop_duplicates(subset = 'FlowName')
             .drop(columns=['location', 'payload', 'inventory', 'activity', 'source_type'],
                   errors='ignore')
             .reset_index(drop=True)
             .assign(amount = 1)
             .assign(ProcessCategory = 'Bridge Processes / USLCI to Heavy Equipment Operations')
             .assign(ProcessName = lambda x: x["FlowName"] + ' BRIDGE, USLCI to Heavy Equipment Operations')
             .assign(ProcessID = lambda x: x['ProcessName'].apply(make_uuid))
             # ^ need more args passed to UUID to avoid duplicates?
             )
df_bridge = (pd.concat([
        df_bridge
            .assign(reference = lambda x: ~x['reference'])
            .assign(IsInput = lambda x: ~x['IsInput'])
            .assign(FlowUUID = lambda x: x['FlowName'].apply(make_uuid)),
        # ^ first chunk is for new flows
        df_bridge
           .assign(FlowName = lambda x: x['fuel'].map(
               {k: v['target_name'] for k, v in fuel_dict.items()}))
           .assign(FlowUUID = lambda x: x['fuel'].map(
               {k: flow_dict.get(v['target_name']).id
                for k, v in fuel_dict.items()
                if v.get('BRIDGE', False)}))
           .assign(unit = lambda x: x['fuel'].map(
               {k: v.get('unit') for k, v in fuel_dict.items()}))
           .assign(conversion = lambda x: x['fuel'].map(
               {k: v.get('conversion', 1) for k, v in fuel_dict.items()}))
           .assign(amount = lambda x: x['amount'] / x['conversion'])
           # ^ apply unit conversion
           .drop(columns=['conversion'])
           .assign(Tag = lambda x: x['fuel'].map(
               {k: list(v.get('repo').keys())[0] for k, v in fuel_dict.items()}))
         # ^ second chunk is for bridged flows
        ], ignore_index=True)
        .drop(columns=['bridge'])
    )

#%% Assign bridge processes as providers where appropriate
df_olca = (df_olca
           .query('not(FlowUUID.isna())')
           .assign(default_provider_process = lambda x: np.where(
               x['bridge'] == True, x["FlowName"] + ' BRIDGE, USLCI to Heavy Equipment Operations',
               ''))
           .assign(default_provider = lambda x: np.where(
               x['bridge'] == True, x['default_provider_process'].apply(make_uuid)
               ,''))
           .drop(columns=['bridge'], errors='ignore')
           .query('equipment in @tech_flows.equipment')
           )

# df_olca.to_csv(parent_path /'moves_processed_output.csv', index=False)

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
from flcac_utils.generate_processes import build_flow_dict, \
    build_process_dict, write_objects, validate_exchange_data

validate_exchange_data(df_olca)
flows, new_flows = build_flow_dict(
    pd.concat([df_olca, df_bridge], ignore_index=True))
# pass bridge processes too to ensure those flows get created

# replace newly created flows with those pulled via API
api_flows = {flow.id: flow for k, flow in flow_dict.items()}
if not(flows.keys() | api_flows.keys()) == flows.keys():
    print('Warning, some flows not consistent')
else:
    flows.update(api_flows)

processes = {}
# loop through each vehicle type and region to adjust metadata before writing processes
for s in df_olca['sector'].unique():
    _df_olca = df_olca.query('sector == @s')
    for r in _df_olca['region'].unique():
        _process_meta = process_meta.copy()
        if r == 'US':
            _process_meta['geography_description'] = _process_meta.get('geography_description_US')
        _process_meta.pop('geography_description_US')
        # _process_meta.pop('vehicle_descriptions')
        for k, v in _process_meta.items():
            if not isinstance(v, str): continue
            v = v.replace('[VEHICLE_TYPE]', s.title())
            v = v.replace('[VEHICLE_CLASS]', s.split(',')[0])
            _process_meta[k] = v
        p_dict = build_process_dict(_df_olca.query('region == @r'),
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
