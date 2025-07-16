import pandas as pd
import random
import math

# ----------------------------------------
# Step 1: Load Input Sheets & Normalize
# ----------------------------------------
excel_path = '/content/BAR-2.xlsx'  # ← adjust if needed

# Floors sheet reader + normalization
def normalize_floor_cols(df):
    mapping = {}
    for c in df.columns:
        key = c.lower().replace(' ', '').replace('_','')
        if 'usable' in key and 'area' in key:
            mapping[c] = 'Usable_Area'
        elif 'capacity' in key or 'loading' in key:
            mapping[c] = 'Max_Assignable_Floor_loading_Capacity'
    return df.rename(columns=mapping)

all_floor_data = pd.read_excel(excel_path, sheet_name='Program Table Input 2 - Floor')
all_floor_data.columns = all_floor_data.columns.str.strip()
all_floor_data = normalize_floor_cols(all_floor_data)

# Blocks sheet
all_block_data = pd.read_excel(excel_path, sheet_name='Existing Program Table Input 1.')
all_block_data.columns = all_block_data.columns.str.strip()

# Department Split + splittable map
department_split_data = pd.read_excel(excel_path, sheet_name='Department Split', skiprows=1)
department_split_data.columns = department_split_data.columns.str.strip()
department_split_data = department_split_data.rename(columns={'BU_Department_Sub-Department': 'Department_Sub-Department'})
split_map = department_split_data.set_index('Department_Sub-Department')['Splittable'].to_dict() if 'Splittable' in department_split_data.columns else {}

# De-centralized logic sheet
logic_df = pd.read_excel(excel_path, sheet_name='De-Centralized Logic', header=None)
De_Centralized_data = {}
current = None
for _, r in logic_df.iterrows():
    label = str(r[0]).strip() if pd.notna(r[0]) else ''
    if label in ['Centralised', 'Semi Centralized', 'DeCentralised']:
        current = label
        De_Centralized_data[current] = {'Add': 0}
    elif current and 'Add into' in label:
        De_Centralized_data[current]['Add'] = int(r[1]) if pd.notna(r[1]) else 0
for key in ['Centralised', 'Semi Centralized', 'DeCentralised']:
    De_Centralized_data.setdefault(key, {'Add': 0})

# Preprocess blocks
immovable_blocks = all_block_data[all_block_data['Immovable-Movable Asset'].str.strip() == 'Immovable Asset']
movable_blocks   = all_block_data[all_block_data['Immovable-Movable Asset'].str.strip() != 'Immovable Asset']
destination_blocks = movable_blocks[movable_blocks['Typical_Destination'].isin(['Destination','both'])]
typical_blocks     = movable_blocks[movable_blocks['Typical_Destination'] == 'Typical']

# Initialize floor assignments
def initialize_floor_assignments(df):
    assigns = {}
    for _, row in df.iterrows():
        floor = row['Name'].strip()
        assigns[floor] = {
            'remaining_area': row['Usable_Area'],
            'remaining_capacity': row['Max_Assignable_Floor_loading_Capacity'],
            'assigned_blocks': [],
            'assigned_departments': set(),
            'ME_area': 0.0,
            'WE_area': 0.0,
            'US_area': 0.0,
            'Support_area': 0.0,
            'Speciality_area': 0.0
        }
    return assigns

floors = list(initialize_floor_assignments(all_floor_data).keys())

# Core assignment function
def run_stack_plan(mode):
    assignments = initialize_floor_assignments(all_floor_data)
    unassigned_blocks = []
    import re
    clean_name = lambda x: re.sub(r'^L\d{3}', '', x).strip()
    floor_map = {clean_name(r['Name']): r['Name'].strip() for _,r in all_floor_data.iterrows()}

    # Assign immovable blocks
    for _, blk in immovable_blocks.iterrows():
        lvl = str(blk['Level']).strip()
        fl = floor_map.get(lvl)
        area = blk.get('Cumulative_Block_Circulation_Area_(SQM)', blk.get('Cumulative_Block_Circulation_Area', 0))
        cap = blk.get('Max_Occupancy_with_Capacity', 0)
        if fl and assignments[fl]['remaining_area'] >= area and assignments[fl]['remaining_capacity'] >= cap:
            assignments[fl]['assigned_blocks'].append(blk.to_dict())
            assignments[fl]['assigned_departments'].add(blk['Department_Sub_Department'])
            assignments[fl]['remaining_area'] -= area
            assignments[fl]['remaining_capacity'] -= cap
        else:
            unassigned_blocks.append(blk.to_dict())

    # Assign destination blocks
    def dest_count():
        if mode == 'centralized': return 2
        key = 'Semi Centralized' if mode == 'semi' else 'DeCentralised'
        return 2 + De_Centralized_data.get(key, {}).get('Add', 0)
    max_dest = min(dest_count(), len(floors))
    groups = {}
    for _, b in destination_blocks.iterrows(): groups.setdefault(b['Destination_Group'], []).append(b.to_dict())
    for blks in groups.values():
        total_area = sum(x.get('Cumulative_Block_Circulation_Area_(SQM)', x.get('Cumulative_Block_Circulation_Area',0)) for x in blks)
        placed = False
        for fl in floors[:max_dest]:
            if assignments[fl]['remaining_area'] >= total_area:
                for x in blks:
                    assignments[fl]['assigned_blocks'].append(x)
                    assignments[fl]['assigned_departments'].add(x['Department_Sub_Department'])
                assignments[fl]['remaining_area'] -= total_area
                placed = True
                break
        if not placed:
            for x in blks:
                a = x.get('Cumulative_Block_Circulation_Area_(SQM)', x.get('Cumulative_Block_Circulation_Area',0))
                for fl in sorted(floors, key=lambda f: assignments[f]['remaining_area'], reverse=True):
                    if assignments[fl]['remaining_area'] >= a:
                        assignments[fl]['assigned_blocks'].append(x)
                        assignments[fl]['assigned_departments'].add(x['Department_Sub_Department'])
                        assignments[fl]['remaining_area'] -= a
                        break
                else:
                    unassigned_blocks.append(x)

    # Assign typical blocks
    for blk in typical_blocks.to_dict('records'):
        dept = blk['Department_Sub_Department'].strip()
        spl = split_map.get(dept, -1)
        area = blk.get('Cumulative_Block_Circulation_Area_(SQM)', blk.get('Cumulative_Block_Circulation_Area',0))
        placed=False
        targets = sorted(floors, key=lambda f: assignments[f]['remaining_area'], reverse=True) if spl != -1 else random.sample(floors, len(floors))
        for fl in targets:
            if assignments[fl]['remaining_area'] >= area:
                assignments[fl]['assigned_blocks'].append(blk)
                assignments[fl]['assigned_departments'].add(dept)
                assignments[fl]['remaining_area'] -= area
                placed=True
                break
        if not placed: unassigned_blocks.append(blk)

    return assignments, unassigned_blocks

# Output builder
def build_outputs(assignments, unassigned_blocks):
    # Detailed
    detailed=[]
    for fl,info in assignments.items():
        for b in info['assigned_blocks']:
            detailed.append({
                'Block_ID':b.get('Block_ID'), 'Floor':fl,
                'Department':b.get('Department_Sub-Department'),'Block_Name':b.get('Block_Name'),
                'Destination_Group':b.get('Destination_Group'),'SpaceMix':b.get('SpaceMix_(ME_WE_US_Support_Speciality)'),
                'Assigned_Area_SQM':b.get('Cumulative_Block_Circulation_Area_(SQM)', b.get('Cumulative_Block_Circulation_Area',0)),
                'Max_Occupancy':b.get('Max_Occupancy_with_Capacity'),'Asset_Type':b.get('Immovable-Movable Asset')
            })
    df_det = pd.DataFrame(detailed)
    # Summary with allowed values
    df_sum = (df_det.groupby('Floor')
              .agg(Assgn_Blocks=('Block_Name','count'), Assgn_Area=('Assigned_Area_SQM','sum'), Total_Occ=('Max_Occupancy','sum'))
              .reset_index())
    # merge allowed usable area and capacity
    floor_allowed = all_floor_data[['Name','Usable_Area','Max_Assignable_Floor_loading_Capacity']].rename(columns={'Name':'Floor',
                                                                                                'Usable_Area':'Allowed_Usable_Area',
                                                                                                'Max_Assignable_Floor_loading_Capacity':'Allowed_Max_Occupancy'})
    df_sum = pd.merge(floor_allowed, df_sum, on='Floor', how='left').fillna({'Assgn_Blocks':0,'Assgn_Area':0,'Total_Occ':0})
    # SpaceMix
    cats=['ME','WE','US','Support','Speciality']; totals={c:len(df_det[df_det['SpaceMix']==c]) for c in cats}
    rows=[]
    for fl in df_sum['Floor']:
        sub=df_det[df_det['Floor']==fl]; tot=len(sub)
        for c in cats:
            cnt=len(sub[sub['SpaceMix']==c])
            rows.append({'Floor':fl,'SpaceMix':c,'Unit_Count_on_Floor':cnt,
                         'Pct_of_Floor_UC':round(cnt/tot*100,2) if tot else 0,
                         'Pct_of_Overall_UC':round(cnt/totals.get(c,1)*100,2)})
    df_space = pd.DataFrame(rows)
    # Unassigned
    ua=[]
    for b in unassigned_blocks:
        ua.append({'Department':b.get('Department_Sub-Department'),'Block_Name':b.get('Block_Name'),
                   'Destination_Group':b.get('Destination_Group'),'SpaceMix':b.get('SpaceMix_(ME_WE_US_Support_Speciality)'),
                   'Area_SQM':b.get('Cumulative_Block_Circulation_Area_(SQM)', b.get('Cumulative_Block_Circulation_Area',0)),
                   'Max_Occupancy':b.get('Max_Occupancy_with_Capacity'),'Asset_Type':b.get('Immovable-Movable Asset')})
    df_un = pd.DataFrame(ua)
    return df_det, df_sum, df_space, df_un

# ----------------------------------------
# Step 7: Execute & Export Three Workbooks
# ----------------------------------------
modes = [('centralized','stack_plan_centralized.xlsx'),
         ('semi','stack_plan_semi_centralized.xlsx'),
         ('decentralized','stack_plan_decentralized.xlsx')]
for mode, fname in modes:
    assigns, unassigned = run_stack_plan(mode)
    det, summ, space, unass = build_outputs(assigns, unassigned)
    with pd.ExcelWriter(fname) as writer:
        det.to_excel(writer, sheet_name='Detailed', index=False)
        summ.to_excel(writer, sheet_name='Floor_Summary', index=False)
        space.to_excel(writer, sheet_name='SpaceMix_By_Units', index=False)
        unass.to_excel(writer, sheet_name='Unassigned', index=False)
print("✅ Generated three Excel files with allowed values: central, semi, and decentralized plans.")