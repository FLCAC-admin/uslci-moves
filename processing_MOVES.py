"""
Pre-processing of MOVES output data
"""

import yaml
import pandas as pd
import numpy as np
from pathlib import Path
import re

auth = False
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

MHD_FILE = '20241211_flcac_mhd_emission_factors'
REFUSE_FILE = '20251126_flcac_refuse_emission_factors'
## Check Teams (Task 3 Transportation Datasets / MOVES) for the latest files

DROP_EF_COLS = ['emission_factor', 'energy_units', 'ef_units', 'units']


def _normalize_refuse(df: pd.DataFrame) -> pd.DataFrame:
    """Align refuse truck CSV columns to the MHD on-road schema."""
    return (
        df.rename(columns={'yearid': 'year', 'pollutantname': 'pollutant'})
        .drop(
            columns=[
                'pollutantid', 'sourcetypeid', 'fueltypeid',
                'emission_factor', 'units',
            ],
            errors='ignore',
        )
    )


def _load_onroad_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load MHD (state-level) and refuse (national) emission factor tables."""
    mhd = pd.read_csv(data_path / f'{MHD_FILE}.csv', thousands=',')
    refuse = _normalize_refuse(
        pd.read_csv(data_path / f'{REFUSE_FILE}.csv', thousands=',')
    )
    return mhd, refuse


df_mhd, df_refuse = _load_onroad_inputs()

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

df_mhd = (df_mhd
          .assign(region = lambda x: x['state'].map(region_dict)))

# MHD: national total + regional aggregates from state-level inventory/activity
df_mhd_agg = pd.concat([
    df_mhd
      .drop(columns=DROP_EF_COLS, errors='ignore')
      .drop(columns=['state', 'region'], errors='ignore')
      .groupby(groupby_cols)
      .agg('sum')
      .assign(region = 'US')
      .reset_index(),
    # ^ first chunk is aggregating all states
    df_mhd
      .drop(columns=DROP_EF_COLS, errors='ignore')
      .drop(columns=['state'], errors='ignore')
      .groupby(groupby_cols + ['region'])
      .agg('sum')
      .reset_index(),
    # ^ second chunk aggregates by region
], ignore_index=True)

# Refuse: national-only (no state column); skip regional rollup
df_refuse_agg = (
    df_refuse
    .drop(columns=DROP_EF_COLS, errors='ignore')
    .groupby(groupby_cols)
    .agg('sum')
    .assign(region='US')
    .reset_index()
)

df = (pd.concat([df_mhd_agg, df_refuse_agg], ignore_index=True)
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

def fix_e85(text):
    return text.replace('e85', 'E85')

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
                                            + (x['fuel'].str.lower()
                                                        .apply(remove_parentheses_substring)
                                                        .apply(fix_e85))
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
           # For fuel values assign fuel as the FlowName
           .assign(FlowName = lambda x: np.where(cond2, x['fuel'], x['FlowName']))
           .assign(location = lambda x: np.where(
               x['region'] == 'US', 'US', None))
           .assign(ProcessName = lambda x: np.where(
               x['region'] == 'US', x['ProcessName'].str.rsplit(',', n=1).str.get(0),
               x['ProcessName']))
           # ^ assign location only for full U.S. process and remove substring
           )


#%% Update the fuel_type_var for each process
from flcac_utils.mapping import prepare_tech_flow_mappings

## Identify mappings for technosphere flows (fuel inputs)
fuel_df = pd.read_csv(data_path / 'MOVES_fuel_mapping.csv')

fuel_dict, flow_objs, provider_dict = prepare_tech_flow_mappings(fuel_df, auth=auth)

#%% apply mappings
from flcac_utils.mapping import apply_tech_flow_mapping, create_bridge_processes

df_olca = apply_tech_flow_mapping(df_olca.rename(columns={'FlowName':'name'}),
                                  fuel_dict, flow_objs, provider_dict)

df_bridge = create_bridge_processes(df_olca, fuel_dict, flow_objs)

# HOT FIX: keep published E85 proxy UUIDs while using current Commons punctuation
# (semicolons). BridgeFlowName / ProcessName come from MOVES_fuel_mapping.csv.
E85_BRIDGE_FLOW = "Ethanol; 85%, disepensed; at pump"
E85_PROXY_NAME = f"{E85_BRIDGE_FLOW} - PROXY"
E85_PROXY_UUID = "10d44fc8-3120-326c-b57b-531ce9f43013"
E85_FLOW_UUID = "6dd72fbb-80ed-3c19-9a25-4a7d6d7534c1"

df_bridge = (df_bridge
             .assign(ProcessID = lambda x: np.where(
                 x['ProcessName'] == E85_PROXY_NAME,
                 E85_PROXY_UUID,
                 x['ProcessID']))
             .assign(FlowUUID = lambda x: np.where(
                 x['FlowName'] == E85_BRIDGE_FLOW,
                 E85_FLOW_UUID,
                 x['FlowUUID']))
             )
df_olca = (df_olca
           .assign(default_provider = lambda x: np.where(
               x['default_provider_process'] == E85_PROXY_NAME,
               E85_PROXY_UUID,
               x['default_provider']))
           .assign(FlowUUID = lambda x: np.where(
               x['FlowName'] == E85_BRIDGE_FLOW,
               E85_FLOW_UUID,
               x['FlowUUID']))
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

# np.where on mixed UUID/NaN columns can leave the string 'nan'; clear invalid providers
for col in ('default_provider', 'default_provider_process'):
    if col in df_olca.columns:
        df_olca[col] = df_olca[col].replace({'nan': None, 'None': None})
        df_olca.loc[df_olca['reference'] == True, col] = None

# df_olca.to_csv(parent_path /'moves_processed_output.csv', index=False)

from flcac_utils.generate_processes import build_flow_dict
flows, new_flows = build_flow_dict(
    pd.concat([df_olca, df_bridge], ignore_index=True))
# pass bridge processes too to ensure those flows get created

# replace newly created flows with those pulled via API
api_flows = {flow.id: flow for k, flow in flow_objs.items()}
if not(flows.keys() | api_flows.keys()) == flows.keys():
    print('Warning, some flows not consistent')
else:
    flows.update(api_flows)

#%% Assign exchange dqi
from flcac_utils.util import format_dqi_score, increment_dqi_value
df_olca['exchange_dqi'] = format_dqi_score(moves_inputs['DQI']['Flow'])
# drop DQI entry for reference flow
df_olca['exchange_dqi'] = np.where(df_olca['reference'] == True,
                                    '', df_olca['exchange_dqi'])

#%% prepare metadata
from copy import deepcopy
from flcac_utils.generate_processes import build_location_dict
from flcac_utils.util import assign_year_to_meta, \
    extract_actors_from_process_meta, extract_dqsystems,\
    extract_sources_from_process_meta, generate_locations_from_exchange_df


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override onto a deep copy of base (dicts only)."""
    out = deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = deepcopy(v)
    return out


def _prepare_process_meta(meta: dict) -> tuple[dict, dict, dict]:
    """Finalize process metadata and extract linked sources/actors."""
    meta = assign_year_to_meta(meta, moves_inputs['Year'])
    if isinstance(meta.get('time_description'), str):
        meta['time_description'] = (
            meta['time_description'].replace('[YEAR]', str(moves_inputs['Year']))
        )
    meta, sources = extract_sources_from_process_meta(
        meta, bib_path=data_path / 'transport.bib')
    meta, actors = extract_actors_from_process_meta(meta)
    meta['dq_entry'] = format_dqi_score(moves_inputs['DQI']['Process'])
    return meta, sources, actors


with open(data_path / 'MOVES_onroad_process_metadata.yaml') as f:
    process_meta_raw = yaml.safe_load(f)
with open(data_path / 'MOVES_refuse_process_metadata.yaml') as f:
    refuse_meta_overrides = yaml.safe_load(f) or {}

refuse_meta_raw = _deep_merge(process_meta_raw, refuse_meta_overrides)
process_meta, source_objs, actor_objs = _prepare_process_meta(deepcopy(process_meta_raw))
refuse_process_meta, refuse_source_objs, refuse_actor_objs = _prepare_process_meta(
    refuse_meta_raw)
source_objs = {**source_objs, **refuse_source_objs}
actor_objs = {**actor_objs, **refuse_actor_objs}
dq_objs = extract_dqsystems(moves_inputs['DQI']['dqSystem'])

# prepare locations
locations = generate_locations_from_exchange_df(df_olca)
location_objs = build_location_dict(df_olca, locations)

#%% Build json file
from flcac_utils.generate_processes import \
    build_process_dict, write_objects, validate_exchange_data

validate_exchange_data(df_olca)

processes = {}
# loop through each vehicle type and region to adjust metadata before writing processes
for s in df_olca['source_type'].unique():
    _df_olca = df_olca.query('source_type == @s')
    meta_for_type = refuse_process_meta if s == 'refuse truck' else process_meta
    vehicle_desc = meta_for_type['vehicle_descriptions'].get(
        re.sub(r'[^a-zA-Z0-9]', '_', s.replace(',','')))
    for i in _df_olca[['region', 'fuel']].drop_duplicates().itertuples(index=False):
        _process_meta = meta_for_type.copy()
        if i.region == 'US':
            _process_meta['geography_description'] = _process_meta.get('geography_description_US')
        _process_meta.pop('geography_description_US', None)
        _process_meta.pop('vehicle_descriptions')
        for k, v in _process_meta.items():
            if not isinstance(v, str): continue
            v = v.replace('[VEHICLE_TYPE]', s.title())
            v = v.replace('[VEHICLE DESCRIPTION]', vehicle_desc or '')
            if i.region in regions:
                v = v.replace('[STATES]', ", ".join(regions[i.region]['states']))
            v = v.replace('[VEHICLE_CLASS]', s.split(',')[0])
            v = v.replace('[PAYLOAD]',
                      str(moves_inputs['payloads'].get(s.split(',')[0])['payload'])
                      )
            v = v.replace('[FUEL]', i.fuel)
            _process_meta[k] = v
        p_dict = build_process_dict(_df_olca.query('region == @i.region '
                                                   'and fuel == @i.fuel'),
                                    flows, meta=_process_meta,
                                       loc_objs=location_objs,
                                       source_objs=source_objs,
                                       actor_objs=actor_objs,
                                       dq_objs=dq_objs,
                                       )
        processes.update(p_dict)
# build bridge processes
bridge_processes = build_process_dict(df_bridge, flows, meta=moves_inputs['Bridge'])

#%% Write to json — MHD onroad and refuse as separate packages
out_path = parent_path / 'output'


def _flow_ids_from_processes(proc_dict: dict) -> set[str]:
    ids: set[str] = set()
    for p in proc_dict.values():
        for e in getattr(p, 'exchanges', None) or []:
            flo = getattr(e, 'flow', None)
            fid = getattr(flo, 'id', None) if flo is not None else None
            if fid:
                ids.add(fid)
    return ids


def _subset_flows(flows_dict: dict, new_flow_ids: list, keep_ids: set[str]):
    flows_sub = {k: v for k, v in flows_dict.items() if k in keep_ids}
    new_sub = [k for k in new_flow_ids if k in keep_ids]
    return flows_sub, new_sub


def _latest_zip(directory: Path, name_stub: str) -> Path:
    zips = sorted(
        directory.glob(f'{name_stub}_olca2.0_*.zip'),
        key=lambda z: z.stat().st_ctime,
    )
    if not zips:
        raise FileNotFoundError(
            f'No zip matching {name_stub}_olca2.0_*.zip in {directory}'
        )
    return zips[-1]


refuse_processes = {
    k: v for k, v in processes.items()
    if 'refuse truck' in (getattr(v, 'name', '') or '').lower()
}
mhd_processes = {
    k: v for k, v in processes.items() if k not in refuse_processes
}

mhd_flow_ids = (
    _flow_ids_from_processes(mhd_processes)
    | _flow_ids_from_processes(bridge_processes)
)
refuse_flow_ids = _flow_ids_from_processes(refuse_processes)
mhd_flows, mhd_new_flows = _subset_flows(flows, new_flows, mhd_flow_ids)
refuse_flows, refuse_new_flows = _subset_flows(flows, new_flows, refuse_flow_ids)

# 1) MHD freight onroad (excludes refuse) — compare to moves_onroad_v1.0.0
# write_objects('moves', mhd_flows, mhd_new_flows, mhd_processes,
#               source_objs, actor_objs, dq_objs, location_objs, bridge_processes,
#               out_path=out_path)
## ^^ Import this file into an empty database with units and flow properties only
## or merge into USLCI and overwrite all existing datasets

# 2) Refuse trucks only (US-national)
write_objects('moves_refuse', refuse_flows, refuse_new_flows, refuse_processes,
              source_objs, actor_objs, dq_objs, location_objs,
              out_path=out_path)

#%% Unzip files to repo
from flcac_utils.util import extract_latest_zip

# extract_latest_zip(_latest_zip(out_path, 'moves'),
#                    parent_path,
#                    output_folder_name=Path('output') / 'moves_onroad_v1.0.0')
extract_latest_zip(_latest_zip(out_path, 'moves_refuse'),
                   parent_path,
                   output_folder_name=Path('output') / 'moves_refuse_v1.0.0')
