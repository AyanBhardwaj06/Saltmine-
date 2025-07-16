import pandas as pd
import random
import math

# ----------------------------------------
# Step 1: Load Input Sheets from C-R1.xlsx
# ----------------------------------------
excel_path = '/content/C-R1.xlsx'  # adjust as needed

# 1.1 Floors sheet
all_floor_data = pd.read_excel(
    excel_path,
    sheet_name='Program Table Input 2 - Floor'
)
all_floor_data.columns = all_floor_data.columns.str.strip()
all_floor_data = all_floor_data.rename(columns={
    all_floor_data.columns[0]: 'Name',
    all_floor_data.columns[1]: 'Usable_Area_SQM',
    all_floor_data.columns[2]: 'Max_Capacity'
})
all_floor_data['Usable_Area_SQM'] = pd.to_numeric(all_floor_data['Usable_Area_SQM'], errors='raise')
all_floor_data['Max_Capacity']   = pd.to_numeric(all_floor_data['Max_Capacity'],   errors='raise')

# 1.2 Blocks sheet: normalize and map
blk = pd.read_excel(excel_path, sheet_name='Program Table Input 1 - Block')
blk.columns = (
    blk.columns
       .str.strip()
       .str.lower()
       .str.replace(r'[^\w]+', '_', regex=True)
       .str.strip('_')
)
# detect key columns dynamically
cum_col = next(c for c in blk.columns if 'cumulative' in c and 'circulation' in c)
occ_col = next(c for c in blk.columns if 'max' in c and 'capacity' in c)
td_col  = next(c for c in blk.columns if 'typical' in c and 'destination' in c)
hn_col  = next(c for c in blk.columns if 'neighborhood' in c)
sm_col  = next(c for c in blk.columns if 'spacemix' in c)
all_block_data = blk.copy()
all_block_data['cumulative_area_sqm'] = pd.to_numeric(all_block_data[cum_col], errors='coerce')
all_block_data['max_occupancy']       = pd.to_numeric(all_block_data[occ_col], errors='coerce')
all_block_data['typical_destination'] = all_block_data[td_col].astype(str).str.strip()
all_block_data['neighborhood']        = all_block_data[hn_col].astype(str).str.strip()
all_block_data['spacemix']            = all_block_data[sm_col].astype(str).str.strip()
# ensure level columns exist
for lvl in ['level_1','level_2','level_3','level_1_area','level_2_area','level_3_area']:
    if lvl not in all_block_data.columns:
        all_block_data[lvl] = None

# 1.3 Department Split sheet: dynamic header
dept_df = pd.read_excel(excel_path, sheet_name='Department Split', header=None)
hdr = dept_df.iloc[0].fillna('').astype(str).str.strip().tolist()
dept_df.columns = hdr
dept_df = dept_df.iloc[1:].reset_index(drop=True)
dep_col = next(c for c in hdr if 'department' in c.lower() and 'sub' in c.lower())
spl_col = next(c for c in hdr if 'splittable' in c.lower())
min_col = next(c for c in hdr if '%' in c and 'min' in c.lower())
dept_splittable = dept_df.set_index(dep_col)[spl_col].astype(int).to_dict()
dept_min_pct    = dept_df.set_index(dep_col)[min_col].astype(float).to_dict()

# 1.4 Adjacency sheets
xls = pd.ExcelFile(excel_path)
adj_sheets = [s for s in xls.sheet_names if 'Adjacency' in s]
adj_sub_df = pd.read_excel(excel_path,
                           sheet_name=[s for s in adj_sheets if 'Neighborhood' not in s][0],
                           header=1, index_col=0)
adj_sub_df = adj_sub_df.apply(pd.to_numeric, errors='coerce')
adj_nh_df = None
nh_sheets = [s for s in adj_sheets if 'Neighborhood' in s]
if nh_sheets:
    adj_nh_df = pd.read_excel(excel_path, sheet_name=nh_sheets[0], header=1, index_col=0)
    adj_nh_df = adj_nh_df.apply(pd.to_numeric, errors='coerce')

# 1.5 De-Centralized Logic sheet
logic_df = pd.read_excel(excel_path, sheet_name='De-Centralized Logic', header=None)
DeC_data, current = {}, None
for _, row in logic_df.iterrows():
    key = str(row[0]).strip()
    if key in ['Centralised','Semi Centralized','DeCentralised']:
        current = key
        DeC_data[current] = {'Add': 0}
    elif current and '( Add' in key:
        DeC_data[current]['Add'] = int(row[1]) if pd.notna(row[1]) else 0
for k in ['Centralised','Semi Centralized','DeCentralised']:
    DeC_data.setdefault(k, {'Add': 0})

# ----------------------------------------
# Step 2: Split Destination vs. Typical
# ----------------------------------------
dest_blocks = all_block_data[all_block_data['typical_destination'] == 'Destination']
typ_blocks  = all_block_data[all_block_data['typical_destination'] == 'Typical']

# Helper: initialize floors

def init_floors():
    floors_dict = {}
    for _, r in all_floor_data.iterrows():
        fl = r['Name'].strip()
        floors_dict[fl] = {
            'remaining_area': r['Usable_Area_SQM'],
            'remaining_capacity': r['Max_Capacity'],
            'assigned_blocks': [],
            'assigned_departments': set(),
            'ME_area': 0.0,
            'WE_area': 0.0,
            'US_area': 0.0,
            'Support_area': 0.0,
            'Speciality_area': 0.0
        }
    return floors_dict

floors = list(init_floors().keys())

def dest_floor_count(mode):
    if mode == 'centralized':
        return 2
    if mode == 'semi':
        return 2 + DeC_data['Semi Centralized']['Add']
    if mode == 'decentralized':
        return 2 + DeC_data['DeCentralised']['Add']
    return 2

# Main stacking function

def run_stack_plan(mode):
    assignments = init_floors()
    unassigned = []

    # Phase 1: Destination groups
    grp_info = {}
    for _, blk in dest_blocks.iterrows():
        grp = blk['destination_group']
        grp_info.setdefault(grp, {'blocks': [], 'area': 0, 'cap': 0})
        d = blk.to_dict()
        grp_info[grp]['blocks'].append(d)
        grp_info[grp]['area'] += d['cumulative_area_sqm']
        grp_info[grp]['cap'] += d['max_occupancy']

    limit = min(dest_floor_count(mode), len(floors))
    for info in grp_info.values():
        placed = False
        # try primary floors
        for fl in floors[:limit]:
            if (assignments[fl]['remaining_area'] >= info['area'] and
                assignments[fl]['remaining_capacity'] >= info['cap']):
                for b in info['blocks']:
                    assignments[fl]['assigned_blocks'].append(b)
                    assignments[fl]['remaining_area'] -= b['cumulative_area_sqm']
                    assignments[fl]['remaining_capacity'] -= b['max_occupancy']
                    assignments[fl]['assigned_departments'].add(b['department_sub_department'].strip())
                placed = True
                break
        # try other floors
        if not placed:
            for fl in floors[limit:]:
                if (assignments[fl]['remaining_area'] >= info['area'] and
                    assignments[fl]['remaining_capacity'] >= info['cap']):
                    for b in info['blocks']:
                        assignments[fl]['assigned_blocks'].append(b)
                        assignments[fl]['remaining_area'] -= b['cumulative_area_sqm']
                        assignments[fl]['remaining_capacity'] -= b['max_occupancy']
                        assignments[fl]['assigned_departments'].add(b['department_sub_department'].strip())
                    placed = True
                    break
        # block-by-block fallback
        if not placed:
            for b in sorted(info['blocks'], key=lambda x: x['cumulative_area_sqm'], reverse=True):
                for fl in sorted(floors, key=lambda f: assignments[f]['remaining_area'], reverse=True):
                    if (assignments[fl]['remaining_area'] >= b['cumulative_area_sqm'] and
                        assignments[fl]['remaining_capacity'] >= b['max_occupancy']):
                        assignments[fl]['assigned_blocks'].append(b)
                        assignments[fl]['remaining_area'] -= b['cumulative_area_sqm']
                        assignments[fl]['remaining_capacity'] -= b['max_occupancy']
                        assignments[fl]['assigned_departments'].add(b['department_sub_department'].strip())
                        break
                else:
                    unassigned.append(b)

    # Phase 2: Typical – Neighborhood first
    nh_groups, rest = {}, []
    for blk in typ_blocks.to_dict('records'):
        nh = blk['neighborhood']
        if nh and nh != 'nan':
            nh_groups.setdefault(nh, []).append(blk)
        else:
            rest.append(blk)
    # assign neighborhood groups
    for group in nh_groups.values():
        area = sum(x['cumulative_area_sqm'] for x in group)
        cap = sum(x['max_occupancy'] for x in group)
        for fl in sorted(floors, key=lambda f: assignments[f]['remaining_area'], reverse=True):
            if (assignments[fl]['remaining_area'] >= area and
                assignments[fl]['remaining_capacity'] >= cap):
                for x in group:
                    assignments[fl]['assigned_blocks'].append(x)
                    assignments[fl]['remaining_area'] -= x['cumulative_area_sqm']
                    assignments[fl]['remaining_capacity'] -= x['max_occupancy']
                    assignments[fl]['assigned_departments'].add(x['department_sub_department'].strip())
                    cat = x['spacemix']
                    if cat == 'ME':       assignments[fl]['ME_area']       += x['cumulative_area_sqm']
                    elif cat == 'WE':     assignments[fl]['WE_area']       += x['cumulative_area_sqm']
                    elif cat == 'US':     assignments[fl]['US_area']       += x['cumulative_area_sqm']
                    elif cat.lower()=='support':    assignments[fl]['Support_area']    += x['cumulative_area_sqm']
                    elif cat.lower()=='speciality': assignments[fl]['Speciality_area'] += x['cumulative_area_sqm']
                break
        else:
            unassigned.extend(group)

    # Phase 2.2: Department unsplittable
    dept_groups, splittable = {}, []
    for blk in rest:
        key = blk['department_sub_department'].strip()
        if dept_splittable.get(key, -1) == -1:
            splittable.append(blk)
        else:
            dept_groups.setdefault(key, []).append(blk)
    # assign full dept groups
    for blks in dept_groups.values():
        area = sum(x['cumulative_area_sqm'] for x in blks)
        cap = sum(x['max_occupancy'] for x in blks)
        for fl in sorted(floors, key=lambda f: assignments[f]['remaining_area'], reverse=True):
            if (assignments[fl]['remaining_area'] >= area and
                assignments[fl]['remaining_capacity'] >= cap):
                for x in blks:
                    assignments[fl]['assigned_blocks'].append(x)
                    assignments[fl]['remaining_area'] -= x['cumulative_area_sqm']
                    assignments[fl]['remaining_capacity'] -= x['max_occupancy']
                    assignments[fl]['assigned_departments'].add(key)
                    cat = x['spacemix']
                    if cat == 'ME':       assignments[fl]['ME_area']       += x['cumulative_area_sqm']
                    elif cat == 'WE':     assignments[fl]['WE_area']       += x['cumulative_area_sqm']
                    elif cat == 'US':     assignments[fl]['US_area']       += x['cumulative_area_sqm']
                    elif cat.lower()=='support':    assignments[fl]['Support_area']    += x['cumulative_area_sqm']
                    elif cat.lower()=='speciality': assignments[fl]['Speciality_area'] += x['cumulative_area_sqm']
                break
        else:
            unassigned.extend(blks)

    # Phase 2.3: Splittable categories (ME first, then others)
    # ME
    me_blocks = [x for x in splittable if x['spacemix']=='ME']
    random.shuffle(me_blocks)
    for x in me_blocks:
        area, cap = x['cumulative_area_sqm'], x['max_occupancy']
        for fl in random.sample(floors, len(floors)):
            if assignments[fl]['remaining_area'] >= area:
                assignments[fl]['assigned_blocks'].append(x)
                assignments[fl]['remaining_area'] -= area
                assignments[fl]['remaining_capacity'] -= cap
                assignments[fl]['assigned_departments'].add(x['department_sub_department'].strip())
                assignments[fl]['ME_area'] += area
                break
        else:
            unassigned.append(x)
    # proportionally distribute others
    me_counts = {fl: sum(1 for b in assignments[fl]['assigned_blocks'] if b['spacemix']=='ME') for fl in floors}
    total_me = sum(me_counts.values())
    fract = {fl: (me_counts[fl]/total_me if total_me else 1/len(floors)) for fl in floors}
    categories = ['WE','US','Support','Speciality']
    for cat in categories:
        cat_blocks = [x for x in splittable if x['spacemix']==cat]
        n = len(cat_blocks)
        if n == 0:
            continue
        targets = {fl: int(round(fract[fl]*n)) for fl in floors}
        diff = n - sum(targets.values())
        # adjust rounding
        if diff > 0:
            # add to highest fractional parts
            fracs = {fl: fract[fl]*n - targets[fl] for fl in floors}
            for fl in sorted(fracs, key=fracs.get, reverse=True)[:diff]:
                targets[fl] += 1
        elif diff < 0:
            fracs = {fl: fract[fl]*n - targets[fl] for fl in floors}
            for fl in sorted(fracs, key=fracs.get)[: -diff]:
                targets[fl] -= 1
        random.shuffle(cat_blocks)
        counts = {fl: 0 for fl in floors}
        for x in cat_blocks:
            area, cap = x['cumulative_area_sqm'], x['max_occupancy']
            # choose floor with remaining target
            cands = [fl for fl in floors if counts[fl] < targets[fl]] or floors
            for fl in sorted(cands, key=lambda f: targets[f] - counts[f], reverse=True):
                if assignments[fl]['remaining_area'] >= area:
                    assignments[fl]['assigned_blocks'].append(x)
                    assignments[fl]['remaining_area'] -= area
                    assignments[fl]['remaining_capacity'] -= cap
                    assignments[fl]['assigned_departments'].add(x['department_sub_department'].strip())
                    if cat == 'WE': assignments[fl]['WE_area'] += area
                    elif cat == 'US': assignments[fl]['US_area'] += area
                    elif cat == 'Support': assignments[fl]['Support_area'] += area
                    elif cat == 'Speciality': assignments[fl]['Speciality_area'] += area
                    counts[fl] += 1
                    break
            else:
                unassigned.append(x)

    # Phase 3: Build output DataFrames
    # Detailed assignments
    detailed = []
    for fl, info in assignments.items():
        for b in info['assigned_blocks']:
            detailed.append({
                'Floor': fl,
                'Department': b['department_sub_department'],
                'Block_Name': b['block_name'],
                'Destination_Group': b['destination_group'],
                'SpaceMix': b['spacemix'],
                'Assigned_Area_SQM': b['cumulative_area_sqm'],
                'Max_Occupancy': b['max_occupancy']
            })
    detailed_df = pd.DataFrame(detailed)

    # Floor summary
    floor_summary_df = (
        detailed_df
        .groupby('Floor')
        .agg(
            Assgn_Blocks=('Block_Name','count'),
            Assgn_Area_SQM=('Assigned_Area_SQM','sum'),
            Total_Occupancy=('Max_Occupancy','sum')
        )
        .reset_index()
    )
    base = all_floor_data.rename(columns={
        'Name':'Floor',
        'Usable_Area_SQM':'Input_Usable_Area_SQM',
        'Max_Capacity':'Input_Max_Capacity'
    })[['Floor','Input_Usable_Area_SQM','Input_Max_Capacity']]
    floor_summary_df = base.merge(floor_summary_df, on='Floor', how='left').fillna(0)

        # Space mix by units
    cats = ['ME','WE','US','Support','Speciality']
    total_per_cat = {cat: len(typ_blocks[typ_blocks['spacemix']==cat]) for cat in cats}
    rows = []
    for fl in floors:
        cnts = {cat: sum(1 for b in assignments[fl]['assigned_blocks'] if b['spacemix']==cat) for cat in cats}
        for cat in cats:
            pct = (cnts[cat] / total_per_cat[cat] * 100) if total_per_cat[cat] else 0
            rows.append({'Floor': fl, 'SpaceMix': cat, '%spaceMix': round(pct, 2)})
    space_mix_df = pd.DataFrame(rows)

    # Unassigned blocks
    unassigned_df = pd.DataFrame([
        {
            'Department': b['department_sub_department'],
            'Block_Name': b['block_name'],
            'Destination_Group': b['destination_group'],
            'SpaceMix': b['spacemix'],
            'Area_SQM': b['cumulative_area_sqm'],
            'Max_Occupancy': b['max_occupancy']
        } for b in unassigned
    ])

    return detailed_df, floor_summary_df, space_mix_df, unassigned_df

# ----------------------------------------
# Step 4: Generate & Export all 3 plans
# ----------------------------------------
plans = [
    ('centralized','stack_plan_centralized28.xlsx'),
    ('semi','stack_plan_semi_centralized28.xlsx'),
    ('decentralized','stack_plan_decentralized28.xlsx')
]
for mode, fname in plans:
    det, fs, sm, un = run_stack_plan(mode)
    with pd.ExcelWriter(fname) as writer:
        det.to_excel(writer, sheet_name='Detailed', index=False)
        fs.to_excel(writer, sheet_name='Floor_Summary', index=False)
        sm.to_excel(writer, sheet_name='SpaceMix_By_Units', index=False)
        un.to_excel(writer, sheet_name='Unassigned', index=False)
    print(f"✅ Generated {fname}")