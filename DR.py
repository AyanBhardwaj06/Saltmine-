import pandas as pd
import random
import math

# ----------------------------------------
# Step 1: Load Input Sheets & Normalize
# ----------------------------------------

excel_path = '/content/BA- R1.xlsx'  # ← adjust if needed

# 1.1 Floors sheet
all_floor_data = pd.read_excel(
    excel_path,
    sheet_name='Program Table Input 2 - Floor'
)
all_floor_data.columns = all_floor_data.columns.str.strip()

# Normalize usable-area & capacity column names
floor_col_map = {}
for c in all_floor_data.columns:
    key = c.lower().replace(' ', '').replace('_','')
    if 'usable' in key and 'area' in key:
        floor_col_map[c] = 'Usable_Area'
    elif 'capacity' in key or 'loading' in key:
        floor_col_map[c] = 'Max_Assignable_Floor_loading_Capacity'
all_floor_data = all_floor_data.rename(columns=floor_col_map)

# 1.2 Blocks sheet
all_block_data = pd.read_excel(
    excel_path,
    sheet_name='Existing Program Table Input 1.'
)
all_block_data.columns = all_block_data.columns.str.strip()

# 1.3 Department Split sheet
department_split_data = pd.read_excel(
    excel_path,
    sheet_name='Department Split',
    skiprows=1
)
department_split_data.columns = department_split_data.columns.str.strip()
department_split_data = department_split_data.rename(
    columns={'BU_Department_Sub-Department': 'Department_Sub-Department'}
)

# 1.4 Adjacency sheet
xls = pd.ExcelFile(excel_path)
adjacency_sheet_name = [n for n in xls.sheet_names if "Adjacency" in n][0]
raw_adj = xls.parse(adjacency_sheet_name, header=1, index_col=0)
adjacency_data = raw_adj.apply(pd.to_numeric, errors='coerce')
adjacency_data.index = adjacency_data.index.str.strip()
adjacency_data.columns = adjacency_data.columns.str.strip()

# 1.5 De-Centralized Logic sheet
logic_df = pd.read_excel(
    excel_path,
    sheet_name='De-Centralized Logic',
    header=None
)
De_Centralized_data = {}
current = None
for _, r in logic_df.iterrows():
    cell = str(r[0]).strip() if pd.notna(r[0]) else ""
    if cell in ["Centralised", "Semi Centralized", "DeCentralised"]:
        current = cell
        De_Centralized_data[current] = {"Add": 0}
    elif current and "Add into" in cell:
        De_Centralized_data[current]["Add"] = int(r[1]) if pd.notna(r[1]) else 0
for k in ["Centralised", "Semi Centralized", "DeCentralised"]:
    De_Centralized_data.setdefault(k, {"Add": 0})

# ----------------------------------------
# Step 2: Preprocess Blocks
# ----------------------------------------

# Split by asset type
immovable_blocks = all_block_data[all_block_data['Immovable-Movable Asset'].str.strip() == 'Immovable Asset']
movable_blocks   = all_block_data[all_block_data['Immovable-Movable Asset'].str.strip() != 'Immovable Asset']

destination_blocks = movable_blocks[movable_blocks['Typical_Destination'].isin(['Destination','both'])]
typical_blocks     = movable_blocks[movable_blocks['Typical_Destination']=='Typical']

# ----------------------------------------
# Step 3: Initialize Floor Assignments
# ----------------------------------------

def initialize_floor_assignments(floor_df):
    assignments = {}
    for _, row in floor_df.iterrows():
        floor = row['Name'].strip()
        assignments[floor] = {
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
    return assignments

# list of all floors
floors = list(initialize_floor_assignments(all_floor_data).keys())

# ----------------------------------------
# Step 4: Core Assignment Function
# ----------------------------------------

def run_stack_plan(mode):
    # initialize per-run structures
    assignments = initialize_floor_assignments(all_floor_data)
    unassigned_blocks = []

    # helper to map short names
    import re
    def clean_floor_name(f): return re.sub(r'^L\d{3}', '', f).strip()
    floor_name_map = {clean_floor_name(r['Name']): r['Name'].strip() for _,r in all_floor_data.iterrows()}

    # 4.1 Assign immovable blocks by level
    for _, blk in immovable_blocks.iterrows():
        raw = str(blk['Level']).strip()
        fl = floor_name_map.get(raw)
        if fl and assignments[fl]['remaining_area']>=blk['Cumulative_Block_Circulation_Area_(SQM)']:
            assignments[fl]['assigned_blocks'].append(blk.to_dict())
            assignments[fl]['assigned_departments'].add(blk['Department_Sub-Department'])
            assignments[fl]['remaining_area'] -= blk['Cumulative_Block_Circulation_Area_(SQM)']
        else:
            unassigned_blocks.append(blk.to_dict())

    # 4.2 Determine destination floor count
    def dest_count():
        if mode=='centralized': return 2
        key = 'Semi Centralized' if mode=='semi' else 'DeCentralised'
        return 2 + De_Centralized_data.get(key,{}).get('Add',0)
    max_dest = min(dest_count(), len(floors))

    # assign destination groups
    dest_groups = {}
    for _,blk in destination_blocks.iterrows():
        grp = blk['Destination_Group']; dest_groups.setdefault(grp, {'blocks':[]}).update({'blocks': dest_groups.get(grp,{}).get('blocks',[])+[blk.to_dict()]})

    for grp,info in dest_groups.items():
        # try whole
        placed=False
        area=sum(b['Cumulative_Block_Circulation_Area_(SQM)'] for b in info['blocks'])
        for fl in floors[:max_dest]:
            if assignments[fl]['remaining_area']>=area:
                for b in info['blocks']:
                    assignments[fl]['assigned_blocks'].append(b)
                    assignments[fl]['assigned_departments'].add(b['Department_Sub-Department'])
                assignments[fl]['remaining_area']-=area; placed=True; break
        if not placed:
            for b in info['blocks']:
                placed=False
                for fl in sorted(floors, key=lambda f:assignments[f]['remaining_area'], reverse=True):
                    if assignments[fl]['remaining_area']>=b['Cumulative_Block_Circulation_Area_(SQM)']:
                        assignments[fl]['assigned_blocks'].append(b)
                        assignments[fl]['assigned_departments'].add(b['Department_Sub-Department'])
                        assignments[fl]['remaining_area']-=b['Cumulative_Block_Circulation_Area_(SQM)']
                        placed=True; break
                if not placed: unassigned_blocks.append(b)

    # 4.3 Typical blocks: just place until full
    for _,blk in typical_blocks.iterrows():
        placed=False
        for fl in sorted(floors, key=lambda f:assignments[f]['remaining_area'], reverse=True):
            if assignments[fl]['remaining_area']>=blk['Cumulative_Block_Circulation_Area_(SQM)']:
                assignments[fl]['assigned_blocks'].append(blk.to_dict())
                assignments[fl]['assigned_departments'].add(blk['Department_Sub-Department'])
                assignments[fl]['remaining_area']-=blk['Cumulative_Block_Circulation_Area_(SQM)']
                placed=True; break
        if not placed: unassigned_blocks.append(blk.to_dict())

    # Phase 5: Build outputs
    detailed=[]
    for fl,info in assignments.items():
        for b in info['assigned_blocks']:
            detailed.append({
                'Block_ID': b.get('Block_ID'),
                'Floor': fl,
                'Department': b.get('Department_Sub-Department'),
                'Block_Name': b.get('Block_Name'),
                'Destination_Group': b.get('Destination_Group'),
                'SpaceMix': b.get('SpaceMix_(ME_WE_US_Support_Speciality)'),
                'Assigned_Area_SQM': b.get('Cumulative_Block_Circulation_Area_(SQM)'),
                'Max_Occupancy': b.get('Max_Occupancy_with_Capacity'),
                'Asset_Type': b.get('Immovable-Movable Asset')
            })
    detailed_df = pd.DataFrame(detailed)

    floor_sum = (detailed_df.groupby('Floor')
                 .agg(Assgn_Blocks=('Block_Name','count'),
                      Assgn_Area_SQM=('Assigned_Area_SQM','sum'))
                 .reset_index())

    space_rows=[]
    cats=['ME','WE','US','Support','Speciality']
    totals={c:len(detailed_df[detailed_df['SpaceMix']==c]) for c in cats}
    for fl in floors:
        df_fl=detailed_df[detailed_df['Floor']==fl]
        for c in cats:
            cnt=len(df_fl[df_fl['SpaceMix']==c]); tot=totals[c] or 1
            pct_fl=cnt/len(df_fl)*100 if len(df_fl) else 0
            pct_ov=cnt/tot*100
            space_rows.append({'Floor':fl,'SpaceMix':c,'Unit_Count_on_Floor':cnt,
                               'Pct_of_Floor_UC':round(pct_fl,2),'Pct_of_Overall_UC':round(pct_ov,2)})
    space_df=pd.DataFrame(space_rows)

    unass=[{'Department':b.get('Department_Sub-Department'),
            'Block_Name':b.get('Block_Name'),
            'Destination_Group':b.get('Destination_Group'),
            'SpaceMix':b.get('SpaceMix_(ME_WE_US_Support_Speciality)'),
            'Area_SQM':b.get('Cumulative_Block_Circulation_Area_(SQM)'),
            'Max_Occupancy':b.get('Max_Occupancy_with_Capacity'),
            'Asset_Type':b.get('Immovable-Movable Asset')} for b in unassigned_blocks]
    unassigned_df=pd.DataFrame(unass)

    return detailed_df,floor_sum,space_df,unassigned_df

# ----------------------------------------
# Step 6: Run & Export
# ----------------------------------------
central, cen_sum, cen_space, cen_un = run_stack_plan('centralized')
semi, sem_sum, sem_space, sem_un = run_stack_plan('semi')
dec, dec_sum, dec_space, dec_un = run_stack_plan('decentralized')

with pd.ExcelWriter('stack_plan_outputs.xlsx') as w:
    central.to_excel(w,'Central_Detailed',index=False)
    cen_sum.to_excel(w,'Central_Summary',index=False)
    cen_space.to_excel(w,'Central_SpaceMix',index=False)
    cen_un.to_excel(w,'Central_Unassigned',index=False)
    semi.to_excel(w,'Semi_Detailed',index=False)
    sem_sum.to_excel(w,'Semi_Summary',index=False)
    sem_space.to_excel(w,'Semi_SpaceMix',index=False)
    sem_un.to_excel(w,'Semi_Unassigned',index=False)
    dec.to_excel(w,'Dec_Detailed',index=False)
    dec_sum.to_excel(w,'Dec_Summary',index=False)
    dec_space.to_excel(w,'Dec_SpaceMix',index=False)
    dec_un.to_excel(w,'Dec_Unassigned',index=False)

print("✔ Code executed: three stack plan outputs generated.")