"""
Build Commons-ready JSON-LD for GREET MHDV vehicle-materials processes.

One zip for all vehicle classes; all GREET powertrains per class.
Phase A is the sole writer of infra product flows and manufacturing processes.

Composite / each-of GREET rows are split via multi-row tech-flow mapping
(ConversionFactors sum to 1 per SourceFlowName). apply_tech_flow_mapping is
still 1:1 on SourceFlowName, so we explode locally and pass synthetic map keys.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from esupy.util import make_uuid

auth = False
parent_path = Path(__file__).parent
data_path = parent_path / "data"
out_path = parent_path / "output"

#%% Inputs

with open(data_path / "vehicle_infra_inputs.yaml", encoding="utf-8") as f:
    inputs = yaml.safe_load(f)

with open(data_path / "vehicle_infra_process_metadata.yaml", encoding="utf-8") as f:
    process_meta_template = yaml.safe_load(f)

inv = pd.read_csv(data_path / "greet_mhdv_vehicle_materials.csv")
map_df = pd.read_csv(data_path / "vehicle_infra_tech_flow_mapping.csv")

LB_TO_KG = float(inputs["LB_TO_KG"])
MI_TO_KM = float(inputs["MI_TO_KM"])
CUTOFF_CONTEXT = inputs["CUTOFF_CONTEXT"]
powertrain_cols = list(inputs["powertrains"].keys())
vehicle_classes = inputs["vehicle_classes"]

missing_classes = sorted(set(inv["vehicle_class"]) - set(vehicle_classes))
if missing_classes:
    raise KeyError(
        f"vehicle_class not in vehicle_infra_inputs.yaml: {missing_classes}"
    )

# Normalize mapping for multi-row splits (ConversionFactor may be blank historically)
map_df = map_df.copy()
map_df["ConversionFactor"] = pd.to_numeric(
    map_df["ConversionFactor"], errors="coerce"
).fillna(1.0)
map_df["Bridge"] = map_df["Bridge"].fillna(False).astype(bool)
has_target = map_df["TargetFlowName"].fillna("").astype(str).str.strip() != ""
map_df = map_df.loc[has_target].copy()
blank_repo = map_df["TargetRepoName"].fillna("").astype(str).str.strip() == ""
map_df["is_cutoff"] = blank_repo

# Validate conversion sums per source (equal-split / atomic rows should sum to 1)
_cf_sum = map_df.groupby("SourceFlowName")["ConversionFactor"].sum()
_bad_cf = _cf_sum[(_cf_sum - 1.0).abs() > 1e-9]
if len(_bad_cf):
    raise ValueError(
        "Mapping ConversionFactor must sum to 1 per SourceFlowName; bad:\n"
        + _bad_cf.to_string()
    )

# Synthetic 1:1 keys so apply_tech_flow_mapping (dict by SourceFlowName) works
# with multi-row composite sources. Future: teach apply_tech_flow_mapping itself.
map_df = map_df.reset_index(drop=True)
map_df["MapKey"] = [f"__map_{i}__" for i in range(len(map_df))]

#%% Melt powertrains, then explode via mapping (local multi-row handling)

long_rows = []
for pt in powertrain_cols:
    pt_label = inputs["powertrains"][pt]["label"]
    chunk = inv.copy()
    chunk["powertrain"] = pt
    chunk["powertrain_label"] = pt_label
    chunk["amount_lb"] = chunk[pt].astype(float)
    if inputs.get("drop_zero_amount_rows", True):
        chunk = chunk.loc[chunk["amount_lb"] > 0].copy()
    chunk["amount"] = chunk["amount_lb"] * LB_TO_KG
    long_rows.append(chunk)

df = pd.concat(long_rows, ignore_index=True)

mapped_names = set(map_df["SourceFlowName"])
unmapped = sorted(set(df["material"]) - mapped_names)
if unmapped:
    print("WARNING: inventory materials with no mapping rows:", unmapped)

# Explode: one inventory row × N mapping rows (factors applied later by
# apply_tech_flow_mapping via ConversionFactor — do not multiply here).
df_tech = df.merge(
    map_df,
    left_on="material",
    right_on="SourceFlowName",
    how="inner",
    validate="m:m",
)

# Cut-off report uses post-conversion amounts for share math
_rep = df_tech.assign(amount_split=lambda x: x["amount"] * x["ConversionFactor"])
cutoff_report = []
for (vc_label, pt_label), g in _rep.groupby(
    ["vehicle_class", "powertrain_label"], sort=False
):
    total = g["amount_split"].sum()
    cut = g.loc[g["is_cutoff"], "amount_split"].sum()
    cutoff_report.append(
        {
            "vehicle_class": vc_label,
            "powertrain": pt_label,
            "total_kg": total,
            "cutoff_kg": cut,
            "cutoff_share": (cut / total) if total else 0.0,
        }
    )

print("Cut-off mass share (CUTOFF flows; after mapping split, kg):")
for r in cutoff_report:
    print(
        f"  {r['vehicle_class']} / {r['powertrain']}: "
        f"{r['cutoff_share']:.2%} "
        f"({r['cutoff_kg']:.2f} of {r['total_kg']:.2f} kg)"
    )

#%% Process / product names (ILCD; no acronyms)

name_pattern = inputs["NamePattern"]


def process_name(class_label: str, powertrain_label: str) -> str:
    return name_pattern.format(
        class_label=class_label, powertrain_label=powertrain_label
    )


df_tech = (
    df_tech.assign(
        ProcessName=lambda x: [
            process_name(c, p)
            for c, p in zip(x["vehicle_class"], x["powertrain_label"])
        ],
        ProcessCategory=inputs["ProcessCategory"],
        FlowType="PRODUCT_FLOW",
        IsInput=True,
        reference=False,
        unit="kg",
        description=lambda x: x["component"],
        # Synthetic key for apply_tech_flow_mapping (not the GREET material string)
        name=lambda x: x["MapKey"],
        Context="",
        location="US",
    )
)
df_tech["ProcessID"] = df_tech["ProcessName"].apply(make_uuid)

# Reference product row per process
ref_rows = (
    df_tech[["ProcessName", "vehicle_class",
             "powertrain", "powertrain_label", "ProcessCategory", "location"]]
    .drop_duplicates()
    .assign(
        FlowName=lambda x: x["ProcessName"],
        name=lambda x: x["ProcessName"],
        amount=1.0,
        unit=inputs["ProductUnit"],
        FlowType="PRODUCT_FLOW",
        IsInput=False,
        reference=True,
        description="",
        Context=inputs["FlowContext"],
        component="",
        material="",
        is_cutoff=False,
    )
)

#%% Technosphere mapping

from flcac_utils.commons_api import get_single_object
from flcac_utils.mapping import (
    apply_tech_flow_mapping,
    assert_provider_supplies_target_flow,
    prepare_tech_flow_mappings,
)
from flcac_utils.util import extract_processes, norm_uuid

# 1:1 synthetic SourceFlowName = MapKey for flcac_utils (multi-row safe locally)
tech_map = map_df.copy()
tech_map["SourceFlowName"] = tech_map["MapKey"]
for col in ("TargetRepoName", "TargetFlowUUID", "Provider", "ProviderUUID"):
    tech_map[col] = tech_map[col].replace("", np.nan)

# flcac-utils resolves Commons flows by name only; duplicate names (e.g. GFRP
# manufacturing vs waste) need TargetFlowUUID. Clear Provider during prepare so
# the provider check doesn't run against the wrong name-resolved UUID, then
# re-bind UUID + provider locally (no flcac-utils change).
uuid_mask = tech_map["TargetFlowUUID"].notna() & tech_map["TargetRepoName"].notna()
saved_uuid_rows = tech_map.loc[
    uuid_mask,
    [
        "SourceFlowName",
        "TargetRepoName",
        "TargetFlowName",
        "TargetFlowUUID",
        "Provider",
        "ProviderUUID",
    ],
].copy()
tech_map_prep = tech_map.copy()
tech_map_prep.loc[uuid_mask, "Provider"] = np.nan

mat_dict, flow_objs, provider_dict = prepare_tech_flow_mappings(
    tech_map_prep, auth=auth
)

extra_providers: dict[str, list[str]] = {}
flow_by_uuid: dict[tuple[str, str], object] = {}
for _, row in saved_uuid_rows.iterrows():
    key = row["SourceFlowName"]
    if key not in mat_dict:
        continue
    want = norm_uuid(row["TargetFlowUUID"])
    if not want:
        continue
    cache_key = (row["TargetRepoName"], want)
    if cache_key not in flow_by_uuid:
        flow_by_uuid[cache_key] = get_single_object(
            row["TargetRepoName"], "FLOW", want, auth=auth
        )
    flo = flow_by_uuid[cache_key]
    flow_objs[row["TargetFlowName"]] = flo
    mat_dict[key]["id"] = flo.id
    pname = row["Provider"]
    if pd.isna(pname) or not str(pname).strip():
        continue
    mat_dict[key]["provider"] = pname
    if pname not in provider_dict:
        extra_providers.setdefault(row["TargetRepoName"], []).append(pname)

if extra_providers:
    provider_dict.update(
        extract_processes(extra_providers, to_ref=False, auth=auth)
    )

for _, row in saved_uuid_rows.iterrows():
    key = row["SourceFlowName"]
    pname = row["Provider"]
    if key not in mat_dict or pd.isna(pname) or not str(pname).strip():
        continue
    tid = mat_dict[key].get("id")
    proc = provider_dict.get(pname)
    if not tid or proc is None:
        continue
    assert_provider_supplies_target_flow(
        proc,
        str(tid),
        provider_name=pname,
        target_flow_name=mat_dict[key]["name"],
        source_flow_name=key,
    )

df_olca = apply_tech_flow_mapping(df_tech, mat_dict, flow_objs, provider_dict)

# New CUTOFF flows: no Commons UUID/provider → assign UUID + CUTOFF context
cutoff_mask = (
    df_olca["FlowUUID"].isna()
    & (df_olca["FlowType"] != "ELEMENTARY_FLOW")
    & ~df_olca["reference"].fillna(False).astype(bool)
)
df_olca.loc[cutoff_mask, "Context"] = CUTOFF_CONTEXT
df_olca.loc[cutoff_mask, "FlowUUID"] = df_olca.loc[cutoff_mask].apply(
    lambda z: make_uuid(z["FlowName"], z["Context"]), axis=1
)
df_olca.loc[cutoff_mask, "default_provider"] = ""
df_olca.loc[cutoff_mask, "default_provider_process"] = ""

still_missing = df_olca["FlowUUID"].isna() & ~df_olca["reference"].fillna(False)
if still_missing.any():
    bad = df_olca.loc[still_missing, ["name", "FlowName", "material"]].drop_duplicates()
    print("WARNING: dropping exchanges still missing FlowUUID:")
    print(bad.to_string())
    df_olca = df_olca.loc[~still_missing].copy()

ref_rows = ref_rows.assign(
    ProcessID=lambda x: x["ProcessName"].apply(make_uuid),
    FlowUUID=lambda x: x.apply(
        lambda z: make_uuid(z["FlowName"], z["Context"]), axis=1
    ),
    default_provider="",
    default_provider_process="",
)

df_olca = df_olca.assign(
    ProcessID=lambda x: x["ProcessName"].apply(make_uuid),
)

df_olca = pd.concat([df_olca, ref_rows], ignore_index=True)

#%% Flows + DQI

from flcac_utils.generate_processes import build_flow_dict
from flcac_utils.util import (
    format_dqi_score,
    assign_year_to_meta,
    extract_actors_from_process_meta,
    extract_dqsystems,
    extract_sources_from_process_meta,
    generate_locations_from_exchange_df,
)

flows, new_flows = build_flow_dict(df_olca)
api_flows = {flow.id: flow for k, flow in flow_objs.items()}
flows.update(api_flows)

for fid in new_flows:
    flow = flows.get(fid)
    if flow is None:
        continue
    cat = getattr(flow, "category", "") or ""
    if "CUTOFF" in cat:
        flow.description = "CUTOFF"

n_cutoff_flows = sum(
    1
    for fid in new_flows
    if flows.get(fid) is not None
    and "CUTOFF" in (getattr(flows[fid], "category", "") or "")
)
print(f"New CUTOFF product flows written: {n_cutoff_flows}")

df_olca["exchange_dqi"] = format_dqi_score(inputs["DQI"]["Flow"])
df_olca["exchange_dqi"] = np.where(
    df_olca["reference"] == True, "", df_olca["exchange_dqi"]  # noqa: E712
)

#%% Metadata

process_meta = assign_year_to_meta(process_meta_template, inputs["Year"])
(process_meta, source_objs) = extract_sources_from_process_meta(
    process_meta, bib_path=data_path / "transport.bib"
)
(process_meta, actor_objs) = extract_actors_from_process_meta(process_meta)
dq_objs = extract_dqsystems(inputs["DQI"]["dqSystem"])
process_meta["dq_entry"] = format_dqi_score(inputs["DQI"]["Process"])

locations = generate_locations_from_exchange_df(df_olca)
from flcac_utils.generate_processes import build_location_dict

location_objs = build_location_dict(df_olca, locations)

#%% Build + write single vehicle_infrastructure_v1.0 package

from flcac_utils.generate_processes import (
    build_process_dict,
    write_objects,
    validate_exchange_data,
)
from flcac_utils.util import extract_latest_zip

validate_exchange_data(df_olca)

class_meta_vals = {
    vc_label: {
        "lifetime_vmt": float(vc_cfg["lifetime_vmt_mi"]),
        "lifetime_km": float(vc_cfg["lifetime_vmt_mi"]) * MI_TO_KM,
        "gvwr_lbs": float(vc_cfg["gvwr_lbs"]),
    }
    for vc_label, vc_cfg in vehicle_classes.items()
}

processes = {}
for vc_label, vc_cfg in vehicle_classes.items():
    _df = df_olca.loc[df_olca["vehicle_class"] == vc_label].copy()
    if _df.empty:
        print(f"Skipping empty class {vc_label}")
        continue

    for pt in powertrain_cols:
        pt_label = inputs["powertrains"][pt]["label"]
        _p = _df.loc[_df["powertrain"] == pt]
        if _p.empty:
            continue
        _meta = {k: v for k, v in process_meta.items() if k != "vehicle_descriptions"}
        vals = class_meta_vals[vc_label]
        tech = process_meta["vehicle_descriptions"][vc_label]
        for k, v in list(_meta.items()):
            if not isinstance(v, str):
                continue
            v = v.replace("[VEHICLE_CLASS]", vc_label)
            v = v.replace("[POWERTRAIN]", pt_label)
            v = v.replace("[VEHICLE_TECH]", tech)
            v = v.replace("[LIFETIME_VMT]", f"{vals['lifetime_vmt']:g}")
            v = v.replace("[LIFETIME_KM]", f"{vals['lifetime_km']:g}")
            v = v.replace("[GVWR_LBS]", f"{vals['gvwr_lbs']:g}")
            _meta[k] = v
        processes.update(
            build_process_dict(
                _p,
                flows,
                meta=_meta,
                loc_objs=location_objs,
                source_objs=source_objs,
                actor_objs=actor_objs,
                dq_objs=dq_objs,
            )
        )

stub = "vehicle_infrastructure_v1.0"
write_objects(
    stub,
    flows,
    new_flows,
    processes,
    source_objs,
    actor_objs,
    dq_objs,
    location_objs,
    out_path=out_path,
)
extract_latest_zip(
    out_path,
    parent_path,
    output_folder_name=Path("output") / stub,
)
print(f"Wrote {stub}: {len(processes)} processes")
print("Done.")
