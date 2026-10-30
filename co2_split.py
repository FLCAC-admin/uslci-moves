"""
Split MOVES Atmospheric CO2 (gross) into FEDEFL fossil vs biogenic flows.
"""

import pandas as pd

FOSSIL_SOURCE_FLOW = 'Atmospheric CO2'
BIOGENIC_SOURCE_FLOW = 'Atmospheric CO2, biogenic'


def split_co2_by_fossil_fraction(df, fossil_fraction_path, flow_mapping_path):
    """
    Split mapped Atmospheric CO2 by fuel fossilFraction.

    fossil CO2 = amount * fossilFraction
    biogenic CO2 = amount * (1 - fossilFraction)
    """
    fractions = (pd.read_csv(fossil_fraction_path)
                 .set_index('fuel')['fossilFraction'].astype(float).to_dict())
    mapping = pd.read_csv(flow_mapping_path).set_index('SourceFlowName')
    fossil = mapping.loc[FOSSIL_SOURCE_FLOW]
    biogenic = mapping.loc[BIOGENIC_SOURCE_FLOW]

    is_co2 = df['FlowName'].eq(fossil['TargetFlowName'])
    co2 = df.loc[is_co2].copy()
    other = df.loc[~is_co2]
    frac = co2['fuel'].map(fractions).fillna(1.0)

    fossil_rows = co2.copy()
    fossil_rows['amount'] = co2['amount'] * frac

    bio_mask = frac < 1.0
    parts = [other, fossil_rows]
    if bio_mask.any():
        bio_rows = co2.loc[bio_mask].copy()
        bio_rows['amount'] = co2.loc[bio_mask, 'amount'] * (1.0 - frac.loc[bio_mask])
        bio_rows['FlowName'] = biogenic['TargetFlowName']
        bio_rows['FlowUUID'] = biogenic['TargetFlowUUID']
        bio_rows['Context'] = biogenic['TargetFlowContext']
        parts.append(bio_rows)

    return pd.concat(parts, ignore_index=True)
