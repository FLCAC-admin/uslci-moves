"""
Pre-processing of MOVES output data
"""

import yaml
import pandas as pd
import numpy as np
from pathlib import Path
import re

auth = True
parent_path = Path(__file__).parent
data_path =  parent_path / 'data'

## Read in MOVES data

groupby_cols = ['year',
                'source_type',
                'fuel',
                'pollutant',
                'payload', # units are short tons
                ]

activity_cols = ['inventory', # units are kg (or mmbtu for energy)
                 'activity', # units are km
                 ]

file = '20241211_flcac_mhd_emission_factors'
## Check Teams (Task 3 Transportation Datasets / MOVES) for the latest file
df_orig = pd.read_csv(data_path / f'{file}.csv', thousands=',')

with open(data_path / "moves_inputs.yaml", "r") as file:
    moves_inputs = yaml.safe_load(file)

SHORT_TON_to_metric_tonne = 0.907185
energy_flow = moves_inputs['EnergyFlow']

#%%
with open(data_path / "moves_regions.yaml", "r") as file:
    regions = yaml.safe_load(file)
    region_dict = {state: region for region, l in regions.items() for state in l['states']}
    elec_grid = {region: {'Name': v['electricity'],
                          'UUID': v['UUID']} for region, v in regions.items()}

df = (df_orig
      .assign(region = lambda x: x['state'].map(region_dict)))

df = (pd.concat([
        df
          .drop(columns=['emission_factor', 'energy_units', 'ef_units'], errors='ignore')
          .drop(columns=['state', 'region'], errors='ignore')
          .groupby(groupby_cols)
          .agg('sum')
          .assign(region = 'US')
          .reset_index(),
        # ^ first chunk is aggregating all states
        df
          .drop(columns=['emission_factor', 'energy_units', 'ef_units'], errors='ignore')
          .drop(columns=['state'], errors='ignore')
          .groupby(groupby_cols + ['region'])
          .agg('sum')
          .reset_index()],
        # ^ second chunk aggregates by region
        ignore_index=True)
      .assign(EF = lambda x: x['inventory'] / (x['activity'] * x['payload'] *
                                               SHORT_TON_to_metric_tonne))
      .assign(Unit = lambda x: np.where(
          x['pollutant'] == energy_flow, 'btu', 'kg'))
      .assign(EF = lambda x: np.where(
          x['pollutant'] == energy_flow, x['EF'] * 1000000, x['EF']))
          # convert mmbtu to btu
      .assign(Context = 'air')
      .assign(description = lambda x:
              np.select([x['pollutant'].str.contains('Brakewear'),
                         x['pollutant'].str.contains('Tirewear'),
                         x['pollutant'].str.contains('Exhaust')],
                        ['Brakeware', 'Tireware', 'Exhaust'], default=''))
          ## ^ add custom description for select flows
      )

#%% Align elementary flows with FEDEFL
from esupy.mapping import apply_flow_mapping
from esupy.util import make_uuid

kwargs = {}
kwargs['material_crosswalk'] = (data_path /
                                'MOVES_fedefl_flow_mapping.csv')
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
      .query('activity > 0')
      )

df = df.query('FlowName != "Water"') # Drop water emissions, all values are 0.

#%% Update the reference_flow_var for each process
def remove_parentheses_substring(text):
    # Remove the substring within parentheses
    cleaned_text = re.sub(r'\(.*?\)', '', text)
    # Remove any extra spaces
    cleaned_text = ' '.join(cleaned_text.split())
    return cleaned_text

# Grab dataset of exisiting UUIDs for processes and technosphere flows to replace
# those that already exist on the commons instead of creating new, where possible
flow_uuids = (pd.read_csv(data_path / 'on_road_uuids.csv')
              .filter(['FlowName', 'flow_uuid']).drop_duplicates()
              .dropna()
              .set_index('FlowName')['flow_uuid'].to_dict()
              )
process_uuids = (pd.read_csv(data_path / 'on_road_uuids.csv')
                 .filter(['ProcessName', 'process_uuid']).drop_duplicates()
                 .dropna()
                 .set_index('ProcessName')['process_uuid'].to_dict()
                 )

df_olca = pd.concat([df,
                     (df[['year', 'source_type', 'fuel', 'payload', 'region']]
                      .drop_duplicates()
                      .assign(reference = True)
                      .assign(IsInput = False)
                      .assign(amount = 1)
                      .assign(FlowName = 'reference_flow_var')
                      .assign(description = '')
                      )], ignore_index=True)
# Update syntax for transport types
df_olca['source_type'] = df_olca['source_type'].map(moves_inputs['tech_flows'])

cond1 = df_olca['FlowName'] == 'reference_flow_var'
cond2 = df_olca['FlowName'] == energy_flow
df_olca = (df_olca
           .assign(ProcessName = lambda x: ('Transport, ' + x['source_type'] + ', '
                                            + x['fuel'].str.lower().apply(remove_parentheses_substring)
                                            + ' powered, ' + x['region']))
           .assign(ProcessCategory = moves_inputs.get('ProcessContext'))
           .assign(ProcessID = lambda x: x['ProcessName'].map(process_uuids)
                   .fillna(x['ProcessName'].apply(make_uuid)))
           # ^^ assign these UUIDs to be based on dict from exisiting USLCI first
           .assign(reference = np.where(cond1, True, False))
           .assign(IsInput = np.where(cond2, True, False))
           .assign(FlowType = np.where(cond1 | cond2, 'PRODUCT_FLOW',
                   'ELEMENTARY_FLOW'))
           .assign(unit = np.where(cond1, 't*km', df_olca['unit']))
           .assign(FlowName = lambda x: np.where(cond1,
                   x['ProcessName'].str.rsplit(',', n=1).str.get(0),
                   x['FlowName']))
           .assign(Context = np.where(cond1, moves_inputs['FlowContext'],
                   df_olca['Context']))
           .assign(FlowUUID = lambda x: np.where(cond1,
                   x['FlowName'].map(flow_uuids).fillna(
                       x.apply(lambda z: make_uuid(z['FlowName'], z['Context']), axis=1)),
                   x['FlowUUID']))
           # ^^ assign these UUIDs to be based on dict from exisiting USLCI
           )


#%% Update the fuel_type_var for each process
from flcac_utils.util import extract_flows, extract_processes

## Identify mappings for technosphere flows (fuel inputs)
fuel_df = pd.read_csv(data_path / 'MOVES_fuel_mapping.csv')
fuel_dict = {row['SourceFlowName']:
                  {'BRIDGE': row['Bridge'],
                   'name': row['BridgeFlowName'] if row['BridgeFlowName'] else row['TargetFlowName'],
                   'provider': row['Provider'] if not row['Bridge'] else np.nan,
                   'repo': {row['TargetRepoName']: row['TargetFlowName']},
                   'conversion': row['ConversionFactor'],
                   'unit': row['TargetUnit']} for _, row in fuel_df.iterrows()}
            ## swap the flow names for bridge processes?

## extract fuel objects in fuel_dict from commons via API
f_dict = {}
p_dict = {}
for k, v in fuel_dict.items():
    if 'repo' in v:
        repo = list(v.get('repo').keys())[0]
        flow = list(v.get('repo').values())[0]
        fuel_dict[k]['target_name'] = flow
        if not fuel_dict[k].get('BRIDGE'):
            fuel_dict[k]['name'] = flow
        if repo in f_dict:
            f_dict[repo].extend([flow])
        else:
            f_dict[repo] = [flow]
        if not pd.isna(v['provider']):
            if repo in p_dict:
                p_dict[repo].extend([v['provider']])
            else:
                p_dict[repo] = [v['provider']]

flow_dict = extract_flows(f_dict, add_tags=False, auth=auth) # don't add tags, all flows are internal
provider_dict = extract_processes(p_dict, to_ref=True, auth=auth)

for k, v in fuel_dict.items():
    if not fuel_dict[k].get('BRIDGE'):
        fuel_dict[k]['id'] = flow_dict.get(v['name']).id
    else:
        fuel_dict[k]['id'] = make_uuid(fuel_dict[k].get('name'))

def create_bridge_name(repo, flowname):
    if repo == 'USLCI':
        return f'{flowname} PROXY'
    else:
        return f'{flowname} BRIDGE, USLCI to {repo}'

def create_bridge_category(repo, flowname):
    if repo == 'USLCI':
        return 'Bridge Processes'
    else:
        return f'Bridge Processes / USLCI to {repo}'

df_olca = (df_olca
           ## For processes that require a bridge process, tag them and add
           # the name of the repository.
           .assign(bridge = lambda x: np.where(
               cond2, x['fuel'].map({k: True for k, v in fuel_dict.items()
                                     if v.get('BRIDGE', False)}),
               False))
           .assign(repo = lambda x: np.where(
               cond2, x['fuel'].map({k: list(v['repo'].keys())[0]
                                     for k, v in fuel_dict.items()
                                     if v.get('BRIDGE', False)}),
               False))

           ## Assign flow information for energy flows
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

df_bridge = (df_olca[cond2]
             .query('bridge == True')
             .drop_duplicates(subset = 'FlowName')
             .drop(columns=['location', 'payload', 'inventory', 'activity', 'source_type'],
                   errors='ignore')
             .reset_index(drop=True)
             .assign(amount = 1)
             .assign(ProcessCategory = lambda x: x.apply(
                 lambda z: create_bridge_category(z['repo'], z['FlowName']), axis=1))
             .assign(ProcessName = lambda x: x.apply(
                 lambda z: create_bridge_name(z['repo'], z['FlowName']), axis=1))
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
           .assign(amount = lambda x: x['amount'] * x['conversion'])
           # ^ apply unit conversion
           .drop(columns=['conversion'])
           .assign(Tag = lambda x: x['fuel'].map(
               {k: list(v.get('repo').keys())[0] for k, v in fuel_dict.items()}))
         # ^ second chunk is for bridged flows

         ## TODO Need to add default providers for these when they are bridges WITHIN
         # a database? Would be nice, but not required
        ], ignore_index=True)
        .drop(columns=['bridge'])
    )

# Assign bridge processes as providers where appropriate
df_olca = (df_olca
           .assign(default_provider_process = lambda x: np.where(
               x['bridge'] == True,
               x.apply(lambda z: create_bridge_name(z['repo'], z['FlowName']), axis=1),
               ''))
           .assign(default_provider = lambda x: np.where(
               x['bridge'] == True, x['default_provider_process'].apply(make_uuid)
               ,''))
           )

cond3 = df_olca['bridge'] != True
# Assign default providers where not a bridge process
df_olca = (df_olca
           .assign(unit = lambda x: np.where(cond2 * cond3, 
               x['fuel'].map(
               {k: v.get('unit') for k, v in fuel_dict.items()}), x['unit']))
           .assign(conversion = lambda x: np.where(cond2 * cond3, 
                x['fuel'].map(
               {k: v.get('conversion', 1) for k, v in fuel_dict.items()}), 1))
           .assign(amount = lambda x: x['amount'] * x['conversion'])
           # ^ apply unit conversion
           .drop(columns=['conversion'])
           .assign(default_provider_process = lambda x: x['FlowName']
                   .map({v['name']: v['provider'] for k, v in fuel_dict.items()
                         if not pd.isna(v['provider'])})
                   .fillna(x['default_provider_process']))
           .assign(default_provider = lambda x: x['default_provider_process']
                   .map({k: v.id for k, v in provider_dict.items()})
                         .fillna(x['default_provider']))
           )

# Assign regional electricity grids as default providers
df_olca = (df_olca
           .assign(default_provider_process = lambda x: np.where(
               x['FlowName'] == 'Electricity, AC, 120 V',
               x['region'].map({k: 'Electricity; at user; consumption mix - ' +
                                v['Name'] for k, v in elec_grid.items()}),
               x['default_provider_process']))
           .assign(default_provider = lambda x: np.where(
               x['FlowName'] == 'Electricity, AC, 120 V',
               x['region'].map({k: v['UUID'] for k, v in elec_grid.items()}),
               x['default_provider']))
           )

df_olca = (df_olca
           .query('not(FlowUUID.isna())')
           .drop(columns=['bridge'], errors='ignore')
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

with open(data_path / 'MOVES_onroad_process_metadata.yaml') as f:
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
for s in df_olca['source_type'].unique():
    _df_olca = df_olca.query('source_type == @s')
    vehicle_desc = process_meta['vehicle_descriptions'].get(
        re.sub(r'[^a-zA-Z0-9]', '_', s.replace(',','')))
    for r in _df_olca['region'].unique():
        _process_meta = process_meta.copy()
        if r == 'US':
            _process_meta['geography_description'] = _process_meta.get('geography_description_US')
        _process_meta.pop('geography_description_US')
        _process_meta.pop('vehicle_descriptions')
        for k, v in _process_meta.items():
            if not isinstance(v, str): continue
            v = v.replace('[VEHICLE_TYPE]', s.title())
            v = v.replace('[VEHICLE DESCRIPTION]', vehicle_desc)
            v = v.replace('[STATES]', ", ".join(regions[r]['states']))
            v = v.replace('[VEHICLE_CLASS]', s.split(',')[0])
            v = v.replace('[PAYLOAD]',
                      str(moves_inputs['payloads'].get(s.split(',')[0])['payload'])
                      )
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
write_objects('moves', flows, new_flows, processes,
              source_objs, actor_objs, dq_objs, location_objs, bridge_processes,
              out_path = out_path)
## ^^ Import this file into an empty database with units and flow properties only
## or merge into USLCI and overwrite all existing datasets
