import pandas as pd
import random
import math

# ----------------------------------------
# Step 1: Load Input Sheets & Normalize
# ----------------------------------------

excel_path = '/content/AA- R2 (1).xlsx'  # ← adjust if needed

# 1.1 Floors sheet (skip first row)
all_floor_data = pd.read_excel(
    excel_path,
    sheet_name='Program Table Input 2 - Floor',
    skiprows=1  # skips the first row (0-indexed)
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
    sheet_name='Renovation Program Table Input '
)
all_block_data.columns = all_block_data.columns.str.strip()

# —————————————————————————————————————————————
# Peel off Immovable Assets if those columns exist
# —————————————————————————————————————————————
if {'Immovable-Movable Asset', 'Level'}.issubset(all_block_data.columns):
    immovable_df = all_block_data.loc[
        all_block_data['Immovable-Movable Asset'].str.strip() == 'Immovable Asset'
    ].copy()
    immovable_df['Assigned_Floor'] = immovable_df['Level'].astype(str).str.strip()
    movable_blocks_df = all_block_data.drop(immovable_df.index).copy()
else:
    immovable_df = pd.DataFrame(columns=list(all_block_data.columns) + ['Assigned_Floor'])
    movable_blocks_df = all_block_data.copy()

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
df_logic = pd.read_excel(
    excel_path,
    sheet_name='De-Centralized Logic',
    header=None
)
De_Centralized_data = {}
current = None
for _, r in df_logic.iterrows():
    cell = str(r[0]).strip() if pd.notna(r[0]) else ""
    if cell in ["Centralised", "Semi Centralized", "DeCentralised"]:
        current = cell
        De_Centralized_data[current] = {"Add": 0}
    elif current and cell == "( Add into cetralised destination Block)":
        De_Centralized_data[current]["Add"] = int(r[1]) if pd.notna(r[1]) else 0
for k in ["Centralised", "Semi Centralized", "DeCentralised"]:
    De_Centralized_data.setdefault(k, {"Add": 0})
# ----------------------------------------
# Step 2: Preprocess Blocks & Department Split
# ----------------------------------------

# 2.1 Convert cumulative circulation area from SQFT → SQM
#all_block_data['Cumulative_Area_SQM'] = (
 #   all_block_data['Cumulative_Block_Circulation_Area_(SQM)']
#)

# Step 0: Assume all_block_data is already defined as your full DataFrame

# Step 1: Select Immovable Asset blocks
immovable_blocks = all_block_data[
    all_block_data['Immovable-Movable Asset'].str.strip() == 'Immovable Asset'
].copy()

# Step 2: Select all other blocks (i.e., NOT immovable → movable or NA or others)
movable_blocks = all_block_data[
    all_block_data['Immovable-Movable Asset'].str.strip() != 'Immovable Asset'
].copy()


# Step 2.1: From Movable blocks, select Destination or both
destination_blocks = movable_blocks[movable_blocks['Typical_Destination'].isin(['Destination', 'both'])].copy()

# Step 2.2: From Movable blocks, select Typical
typical_blocks = movable_blocks[movable_blocks['Typical_Destination'] == 'Typical'].copy()


# ----------------------------------------
# Step 3: Initialize Floor Assignments
# ----------------------------------------

def initialize_floor_assignments(floor_df):
    """
    Returns a dict keyed by floor name. Each entry tracks:
      - remaining_area
      - remaining_capacity
      - assigned_blocks      (list of block‐row dicts)
      - assigned_departments (set of sub‐departments)
      - ME_area, WE_area, US_area, Support_area, Speciality_area (floats)
    """
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

floors = list(initialize_floor_assignments(all_floor_data).keys())

# ----------------------------------------
# Step 4: Core Stacking Function
# ----------------------------------------

def run_stack_plan(mode):
    """
    mode: 'centralized', 'semi', or 'decentralized'
    Returns four DataFrames:
      1) detailed_df      – each block’s assigned floor, department, block name, destination group, space mix, area, occupancy
      2) floor_summary_df – floor‐wise totals (block count, total area, total occupancy)
      3) space_mix_df     – for each floor and each category {ME, WE, US, Support, Speciality}:
                              - Unit_Count_on_Floor
                              - Pct_of_Floor_UC      = (category_count_on_floor / total_blocks_on_floor) × 100%
                              - Pct_of_Overall_UC    = (category_count_on_floor / total_blocks_of_category_overall) × 100%
      4) unassigned_df    – blocks that couldn’t be placed
    """
    assignments = initialize_floor_assignments(all_floor_data)
    unassigned_blocks = []

    import re

    def normalize_floor_name(name):
        """Standardize floor names like 'L002Floor 01' or '3 Floor' → 'Floor 03'"""
        name = str(name).strip()

        # Remove prefix like L001
        name = re.sub(r'^L\d{3}', '', name).strip()

        # If format is like '3 Floor' → convert to 'Floor 03'
        match = re.match(r'^(\d+)\s+Floor$', name)
        if match:
            number = int(match.group(1))
            return f"Floor {number:02d}"

        return name  # for 'Ground Floor', 'Floor 01', etc.
      # Map normalized name → actual floor name in assignments
    floor_name_map = {
        normalize_floor_name(row['Name']): row['Name'].strip()
        for _, row in all_floor_data.iterrows()
    }





    # Assign immovable blocks based on 'Level'
    immovable_blocks = all_block_data[all_block_data['Immovable-Movable Asset'] == 'Immovable Asset'].copy()

    for _, block in immovable_blocks.iterrows():
        target_floor_raw = str(block['Level']).strip()
        target_floor = floor_name_map.get(target_floor_raw, None)

        block_area = block['Cumulative_Block_Circulation_Area']
        block_capacity = block['Max_Occupancy_with_Capacity']

        if target_floor in assignments:
            floor_data = assignments[target_floor]

            # Check area and capacity constraints
            if floor_data['remaining_area'] >= block_area and floor_data['remaining_capacity'] >= block_capacity:
                # Assign block
                floor_data['assigned_blocks'].append(block.to_dict())
                floor_data['assigned_departments'].add(block['Department_Sub_Department'])

                # Update remaining area and capacity
                floor_data['remaining_area'] -= block_area
                floor_data['remaining_capacity'] -= block_capacity

                # Update area category
                category = block['SpaceMix_(ME_WE_US_Support_Speciality)']
                category_key = f"{category}_area"
                if category_key in floor_data:
                    floor_data[category_key] += block_area
            else:
                unassigned_blocks.append(block.to_dict())
        else:
            unassigned_blocks.append(block.to_dict())


    # Determine how many floors to use for destination blocks
    def destination_floor_count():
        if mode == 'centralized':
            return 2
        elif mode == 'semi':
            return 2 + De_Centralized_data["Semi Centralized"]["Add"]
        elif mode == 'decentralized':
            return 2 + De_Centralized_data["DeCentralised"]["Add"]
        else:
            return 2

    max_dest_floors = destination_floor_count()
    # Cap at total number of floors
    max_dest_floors = min(max_dest_floors, len(floors))

    # Pre‐compute each group's total area and total capacity
    dest_groups = {}
    for _, blk in destination_blocks.iterrows():
        grp = blk['Destination_Group']
        if grp not in dest_groups:
            dest_groups[grp] = {'blocks': [], 'total_area': 0.0, 'total_capacity': 0}
        dest_groups[grp]['blocks'].append(blk.to_dict())
        dest_groups[grp]['total_area'] += blk['Cumulative_Block_Circulation_Area']
        dest_groups[grp]['total_capacity'] += blk['Max_Occupancy_with_Capacity']

    # Phase 1: Assign destination groups (try whole‐group first; if that fails, split across floors)
    group_names = list(dest_groups.keys())
    random.shuffle(group_names)
    for grp in group_names:
        info_grp = dest_groups[grp]
        grp_area = info_grp['total_area']
        grp_cap  = info_grp['total_capacity']
        placed_whole = False

        # 4.2.a Attempt to place entire group on any of the first max_dest_floors
        candidate_floors = floors[:max_dest_floors].copy()

        for fl in candidate_floors:
            if (assignments[fl]['remaining_area'] >= grp_area and
                assignments[fl]['remaining_capacity'] >= grp_cap):
                # Entire group fits here—place all blocks
                for blk in info_grp['blocks']:
                    assignments[fl]['assigned_blocks'].append(blk)
                    assignments[fl]['assigned_departments'].add(
                        blk['Department_Sub_Department']
                    )
                assignments[fl]['remaining_area'] -= grp_area
                assignments[fl]['remaining_capacity'] -= grp_cap
                placed_whole = True
                break

        # 4.2.b If not yet placed, try the remaining floors (beyond max_dest_floors)
        if not placed_whole:
            for fl in floors[max_dest_floors:]:
                if (assignments[fl]['remaining_area'] >= grp_area and
                    assignments[fl]['remaining_capacity'] >= grp_cap):
                    for blk in info_grp['blocks']:
                        assignments[fl]['assigned_blocks'].append(blk)
                        assignments[fl]['assigned_departments'].add(
                            blk['Department_Sub_Department'].strip()
                        )
                    assignments[fl]['remaining_area'] -= grp_area
                    assignments[fl]['remaining_capacity'] -= grp_cap
                    placed_whole = True
                    break

        # 4.2.c If still not placed as a whole, split the group block‐by‐block across floors
        if not placed_whole:
            total_remaining_area = sum(assignments[f]['remaining_area'] for f in floors)
            if total_remaining_area >= grp_area:
                # Try placing group by removing the largest blocks one-by-one until remaining can be placed whole
                blocks_sorted = sorted(info_grp['blocks'], key=lambda b: b['Cumulative_Block_Circulation_Area'], reverse=True)
                removed_blocks = []
                trial_blocks = blocks_sorted.copy()

                while trial_blocks:
                    trial_area = sum(b['Cumulative_Block_Circulation_Area'] for b in trial_blocks)
                    trial_capacity = sum(b['Max_Occupancy_with_Capacity'] for b in trial_blocks)

                    # Try to place this reduced group
                    floor_combination = []
                    temp_assignments = {f: assignments[f].copy() for f in floors}
                    temp_floors_by_space = sorted(floors, key=lambda f: assignments[f]['remaining_area'], reverse=True)

                    temp_success = True
                    for blk in trial_blocks:
                        blk_area = blk['Cumulative_Block_Circulation_Area']
                        blk_capacity = blk['Max_Occupancy_with_Capacity']
                        placed_block = False

                        for fl in temp_floors_by_space:
                            if (temp_assignments[fl]['remaining_area'] >= blk_area and
                                temp_assignments[fl]['remaining_capacity'] >= blk_capacity):
                                temp_assignments[fl]['remaining_area'] -= blk_area
                                temp_assignments[fl]['remaining_capacity'] -= blk_capacity
                                floor_combination.append((blk, fl))
                                placed_block = True
                                break

                        if not placed_block:
                            temp_success = False
                            break

                    if temp_success:
                        # Apply final assignment for successfully placed trial blocks
                        for blk, fl in floor_combination:
                            assignments[fl]['assigned_blocks'].append(blk)
                            assignments[fl]['assigned_departments'].add(blk['Department_Sub_Department'].strip())
                            assignments[fl]['remaining_area'] -= blk['Cumulative_Block_Circulation_Area']
                            assignments[fl]['remaining_capacity'] -= blk['Max_Occupancy_with_Capacity']
                        placed_whole = True
                        break
                    else:
                        # Remove one largest block and retry
                        removed_blocks.append(trial_blocks.pop(0))

                # Place removed blocks one-by-one
                for blk in removed_blocks:
                    blk_area = blk['Cumulative_Area_SQM']
                    blk_capacity = blk['Max_Occupancy_with_Capacity']
                    placed_block = False
                    floors_by_space = sorted(floors, key=lambda f: assignments[f]['remaining_area'], reverse=True)

                    for fl in floors_by_space:
                        if (assignments[fl]['remaining_area'] >= blk_area and
                            assignments[fl]['remaining_capacity'] >= bll_capacity):
                            assignments[fl]['assigned_blocks'].append(blk)
                            assignments[fl]['assigned_departments'].add(blk['Department_Sub_Department'].strip())
                            assignments[fl]['remaining_area'] -= blk_area
                            assignments[fl]['remaining_capacity'] -= blk_capacity
                            placed_block = True
                            break

                    if not placed_block:
                        unassigned_blocks.append(blk)
            else:
                # Even splitting won't fit all blocks, place block-by-block
                for blk in sorted(info_grp['blocks'], key=lambda b: b['Cumulative_Area_SQM'], reverse=True):
                    blk_area     = blk['Cumulative_Area_SQM']
                    blk_capacity = blk['Max_Occupancy_with_Capacity']
                    placed_block = False

                    floors_by_space = sorted(floors, key=lambda f: assignments[f]['remaining_area'], reverse=True)
                    for fl in floors_by_space:
                        if (assignments[fl]['remaining_area'] >= blk_area and
                            assignments[fl]['remaining_capacity'] >= blk_capacity):
                            assignments[fl]['assigned_blocks'].append(blk)
                            assignments[fl]['assigned_departments'].add(blk['Department_Sub_Department'].strip())
                            assignments[fl]['remaining_area'] -= blk_area
                            assignments[fl]['remaining_capacity'] -= blk_capacity
                            placed_block = True
                            break

                    if not placed_block:
                        unassigned_blocks.append(blk)

    # Phase 2A: Randomly assign ME blocks (typical)
    me_blocks = [
        blk for blk in typical_blocks.to_dict('records')
        if blk['SpaceMix_(ME_WE_US_Support_Speciality)'].strip() == 'ME'
    ]
    random.shuffle(me_blocks)
    for blk in me_blocks:
        blk_area     = blk['Cumulative_Block_Circulation_Area']
        blk_capacity = blk['Max_Occupancy_with_Capacity']
        blk_dept     = blk['Department_Sub_Department'].strip()

        candidate_floors = floors.copy()
        random.shuffle(candidate_floors)
        placed = False
        for fl in candidate_floors:
            if (assignments[fl]['remaining_area'] >= blk_area and assignments[fl]['remaining_capacity'] >= blk_capacity):
                assignments[fl]['assigned_blocks'].append(blk)
                assignments[fl]['remaining_area'] -= blk_area
                assignments[fl]['remaining_capacity'] -= blk_capacity
                assignments[fl]['assigned_departments'].add(blk_dept)
                assignments[fl]['ME_area'] += blk_area
                placed = True
                break
        if not placed:
            print(f"Warning: Could not place ME block '{blk['Block_Name']}'")

    # Compute ME distribution per floor (unit counts)
    me_count_per_floor = {fl: 0 for fl in floors}
    for fl, info in assignments.items():
        me_count_per_floor[fl] = sum(
            1 for blk in info['assigned_blocks']
            if blk['SpaceMix_(ME_WE_US_Support_Speciality)'].strip() == 'ME'
        )
    total_me = sum(me_count_per_floor.values())
    if total_me == 0:
        me_frac_per_floor = {fl: 1 / len(floors) for fl in floors}
    else:
        me_frac_per_floor = {fl: me_count_per_floor[fl] / total_me for fl in floors}

    # Phase 2B: Assign other categories proportionally
    other_categories = ['WE', 'US', 'Support', 'Speciality']
    for category in other_categories:
        cat_blocks = [
            blk for blk in typical_blocks.to_dict('records')
            if blk['SpaceMix_(ME_WE_US_Support_Speciality)'].strip() == category
        ]
        total_cat = len(cat_blocks)
        if total_cat == 0:
            continue

        raw_targets = {fl: me_frac_per_floor[fl] * total_cat for fl in floors}
        target_counts = {fl: int(round(raw_targets[fl])) for fl in floors}

        diff = total_cat - sum(target_counts.values())
        if diff != 0:
            fractional_parts = {
                fl: raw_targets[fl] - math.floor(raw_targets[fl]) for fl in floors
            }
            if diff > 0:
                for fl in sorted(floors, key=lambda x: fractional_parts[x], reverse=True)[:diff]:
                    target_counts[fl] += 1
            else:
                for fl in sorted(floors, key=lambda x: fractional_parts[x])[: -diff]:
                    target_counts[fl] -= 1

        random.shuffle(cat_blocks)
        assigned_counts = {fl: 0 for fl in floors}

        for blk in cat_blocks:
            blk_area     = blk['Cumulative_Block_Circulation_Area']
            blk_capacity = blk['Max_Occupancy_with_Capacity']
            blk_dept     = blk['Department_Sub_Department'].strip()

            deficits = {fl: target_counts[fl] - assigned_counts[fl] for fl in floors}
            floors_with_deficit = [fl for fl, d in deficits.items() if d > 0]
            if floors_with_deficit:
                candidate_floors = sorted(
                    floors_with_deficit,
                    key=lambda x: deficits[x],
                    reverse=True
                )
            else:
                candidate_floors = floors.copy()

            placed = False
            for fl in candidate_floors:
                if (assignments[fl]['remaining_area'] >= blk_area and assignments[fl]['remaining_capacity'] >= blk_capacity):
                    assignments[fl]['assigned_blocks'].append(blk)
                    assignments[fl]['remaining_area'] -= blk_area
                    assignments[fl]['remaining_capacity'] -= blk_capacity
                    assignments[fl]['assigned_departments'].add(blk_dept)
                    if category == 'WE':
                        assignments[fl]['WE_area'] += blk_area
                    elif category == 'US':
                        assignments[fl]['US_area'] += blk_area
                    elif category == 'Support':
                        assignments[fl]['Support_area'] += blk_area
                    elif category == 'Speciality':
                        assignments[fl]['Speciality_area'] += blk_area
                    assigned_counts[fl] += 1
                    placed = True
                    break

            if not placed:
                fallback = floors.copy()
                random.shuffle(fallback)
                for fl in fallback:
                    if (assignments[fl]['remaining_area'] >= blk_area and assignments[fl]['remaining_capacity'] >= blk_capacity):
                        assignments[fl]['assigned_blocks'].append(blk)
                        assignments[fl]['remaining_area'] -= blk_area
                        assignments[fl]['remaining_capacity'] -= blk_capacity
                        assignments[fl]['assigned_departments'].add(blk_dept)
                        if category == 'WE':
                            assignments[fl]['WE_area'] += blk_area
                        elif category == 'US':
                            assignments[fl]['US_area'] += blk_area
                        elif category == 'Support':
                            assignments[fl]['Support_area'] += blk_area
                        elif category == 'Speciality':
                            assignments[fl]['Speciality_area'] += blk_area
                        assigned_counts[fl] += 1
                        placed = True
                        break

            if not placed:
                print(f"Warning: Could not place {category} block '{blk['Block_Name']}'")
                unassigned_blocks.append(blk)
    # Re-attempt placing unassigned blocks on randomized floor order
    still_unassigned = []

    # Get list of floor names and shuffle
    floor_list = list(assignments.keys())
    random.shuffle(floor_list)  # This randomizes the order

    for block in unassigned_blocks:
        placed = False
        block_area = block['Cumulative_Block_Circulation_Area']
        block_capacity = block['Max_Occupancy_with_Capacity']
        department = block['Department_Sub_Department']
        category = block['SpaceMix_(ME_WE_US_Support_Speciality)']
        category_key = f"{category}_area"

        for floor in floor_list:
            data = assignments[floor]
            if data['remaining_area'] >= block_area and data['remaining_capacity'] >= block_capacity:
                # Assign the block to this floor
                data['assigned_blocks'].append(block)
                data['assigned_departments'].add(department)
                data['remaining_area'] -= block_area
                data['remaining_capacity'] -= block_capacity

                # Update category area
                if category_key in data:
                    data[category_key] += block_area

                placed = True
                break  # Move to next block

        if not placed:
            still_unassigned.append(block)

    # Update the global unassigned_blocks list
    unassigned_blocks = still_unassigned

    # Phase 3: Build Detailed & Summary DataFrames

    # 3.1 Detailed DataFrame
    assignment_list = []
    for fl, info in assignments.items():
        for blk in info['assigned_blocks']:
            assignment_list.append({
                'Block_id': blk['Block_ID'],
                'Floor': fl,
                'Department': blk['Department_Sub_Department'],
                'Block_Name': blk['Block_Name'],
                'Destination_Group': blk['Destination_Group'],
                'SpaceMix': blk['SpaceMix_(ME_WE_US_Support_Speciality)'],
                'Assigned_Area_SQM': blk['Cumulative_Block_Circulation_Area'],
                'Max_Occupancy': blk['Max_Occupancy_with_Capacity'],
                'Asset_Type': blk['Immovable-Movable Asset']  # <-- New column added
            })
    detailed_df = pd.DataFrame(assignment_list)

    # 4.6.2 Floor_Summary DataFrame
     # 3.2 “Floor_Summary” DataFrame
    floor_summary_df = (
    detailed_df
    .groupby('Floor')
    .agg(
        Assgn_Blocks=('Block_Name', 'count'),
        Assgn_Area_SQM=('Assigned_Area_SQM', 'sum'),
        Total_Occupancy=('Max_Occupancy', 'sum')
    )
    .reset_index()
)

    # Merge with original floor input data to get base values
    floor_input_subset = all_floor_data[[
    'Name', 'Usable_Area', 'Max_Assignable_Floor_loading_Capacity'
]].rename(columns={
    'Name': 'Floor',
    'Usable_Area': 'Input_Usable_Area',
    'Max_Assignable_Floor_loading_Capacity': 'Input_Max_Capacity'
})

    # Join input data with summary
    floor_summary_df = pd.merge(
    floor_input_subset,
    floor_summary_df,
    on='Floor',
    how='left'
)

    # Fill NaNs (if any floor didn't get any assignments)
    floor_summary_df[[
    'Assgn_Blocks',
    'Assgn_Area_SQM',
    'Total_Occupancy'
]] = floor_summary_df[[
    'Assgn_Blocks',
    'Assgn_Area_SQM',
    'Total_Occupancy'
]].fillna(0)
    # 3.3 SpaceMix_By_Units DataFrame
    all_categories = ['ME', 'WE', 'US', 'Support', 'Speciality']
    category_totals = {
        cat: len(typical_blocks[
            typical_blocks['SpaceMix_(ME_WE_US_Support_Speciality)'].str.strip() == cat
        ])
        for cat in all_categories
    }

    rows = []
    for fl, info in assignments.items():
        counts = {cat: 0 for cat in all_categories}
        for blk in info['assigned_blocks']:
            cat = blk['SpaceMix_(ME_WE_US_Support_Speciality)'].strip()
            if cat in counts:
                counts[cat] += 1
        total_blocks_on_floor = sum(counts.values())

        for cat in all_categories:
            cnt = counts[cat]
            pct_of_floor = (cnt / total_blocks_on_floor * 100) if total_blocks_on_floor else 0.0
            total_cat = category_totals[cat]
            pct_overall = (cnt / total_cat * 100) if total_cat else 0.0

            rows.append({
                'Floor': fl,
                'SpaceMix': cat,
                'Unit_Count_on_Floor': cnt,
                'Pct_of_Floor_UC': round(pct_of_floor, 2),
                'Pct_of_Overall_UC': round(pct_overall, 2)
            })

    space_mix_df = pd.DataFrame(rows)

     # 4.6.4 Unassigned DataFrame
    unassigned_list = []
    for blk in unassigned_blocks:
        unassigned_list.append({
            'Department': blk.get('Department_Sub_Department', ''),
            'Block_Name': blk.get('Block_Name', ''),
            'Destination_Group': blk.get('Destination_Group', ''),
            'SpaceMix': blk.get('SpaceMix_(ME_WE_US_Support_Speciality)', ''),
            'Area_SQM': blk.get('Cumulative_Area_SQM', 0),
            'Max_Occupancy': blk.get('Max_Occupancy_with_Capacity', 0),
            'Asset_Type': blk['Immovable-Movable Asset']
        })
    unassigned_df = pd.DataFrame(unassigned_list)

    return detailed_df, floor_summary_df, space_mix_df, unassigned_df

# ----------------------------------------
# Step 5: Generate & Export Three Excel Files
# ----------------------------------------
central_detailed, central_floor_sum, central_space_mix, central_unassigned = run_stack_plan('centralized')
semi_detailed, semi_floor_sum, semi_space_mix, semi_unassigned = run_stack_plan('semi')
decentral_detailed, decentral_floor_sum, decentral_space_mix, decentral_unassigned = run_stack_plan('decentralized')


with pd.ExcelWriter('stack_plan_centralized20.xlsx') as writer:
    central_detailed.to_excel(writer, sheet_name='Detailed', index=False)
    central_floor_sum.to_excel(writer, sheet_name='Floor_Summary', index=False)
    central_space_mix.to_excel(writer, sheet_name='SpaceMix_By_Units', index=False)
    central_unassigned.to_excel(writer, sheet_name='Unassigned', index=False)


with pd.ExcelWriter('stack_plan_semi_centralized20.xlsx') as writer:
    semi_detailed.to_excel(writer, sheet_name='Detailed', index=False)
    semi_floor_sum.to_excel(writer, sheet_name='Floor_Summary', index=False)
    semi_space_mix.to_excel(writer, sheet_name='SpaceMix_By_Units', index=False)
    semi_unassigned.to_excel(writer, sheet_name='Unassigned', index=False)


with pd.ExcelWriter('stack_plan_decentralized20.xlsx') as writer:
    decentral_detailed.to_excel(writer, sheet_name='Detailed', index=False)
    decentral_floor_sum.to_excel(writer, sheet_name='Floor_Summary', index=False)
    decentral_space_mix.to_excel(writer, sheet_name='SpaceMix_By_Units', index=False)
    decentral_unassigned.to_excel(writer, sheet_name='Unassigned', index=False)


print("✅ Generated three Excel outputs:")
print("    • stack_plan_centralized8.xlsx")
print("    • stack_plan_semi_centralized8.xlsx")
print("    • stack_plan_decentralized8.xlsx")