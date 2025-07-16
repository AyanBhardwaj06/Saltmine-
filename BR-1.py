import pandas as pd
import random
import math

# ----------------------------------------
# Step 1: Load All Input Sheets from AR-1.xlsx

# ----------------------------------------

excel_path = '/content/BR-1.xlsx'  # ← adjust if needed

# 1.1 Floors sheet (skip the first row)
all_floor_data = pd.read_excel(
    excel_path,
    sheet_name='Program Table Input 2 - Floor',
    skiprows=0  # Don't skip header
)
all_floor_data.columns = all_floor_data.columns.str.strip()
print(all_floor_data.columns.tolist())

all_floor_data = all_floor_data.rename(columns={
    all_floor_data.columns[0]: 'Name',
    all_floor_data.columns[1]: 'Usable_Area_(SQM)',
    all_floor_data.columns[2]: 'Max_Assignable_Floor_loading_Capacity'
})
print(all_floor_data.columns.tolist())
# Coerce floor‐area and capacity to numeric
all_floor_data['Usable_Area_(SQM)'] = pd.to_numeric(
    all_floor_data['Usable_Area_(SQM)'], errors='raise'
)
all_floor_data['Max_Assignable_Floor_loading_Capacity'] = pd.to_numeric(
    all_floor_data['Max_Assignable_Floor_loading_Capacity'], errors='raise'
)

# 1.2 Blocks sheet
all_block_data = pd.read_excel(
    excel_path,
    sheet_name='Program Table Input 1 - Block'
)
all_block_data.columns = all_block_data.columns.str.strip()

# Ensure these columns are numeric
all_block_data['Cumulative_Area_SQM'] = pd.to_numeric(
    all_block_data['Cumulative_Block_Circulation_Area'], errors='raise'
)
all_block_data['Max_Occupancy_with_Capacity'] = pd.to_numeric(
    all_block_data['Max_Occupancy_with_Capacity'], errors='raise'
)

# 1.3 Department Split sheet (skip header row)
department_split_data = pd.read_excel(
    excel_path,
    sheet_name='Department Split',
    skiprows=1 # Skip the first row which is not the header
)
# Set column names explicitly based on expected structure
department_split_data.columns = [
    'Department_Sub-Department',
    'Splittable',
    'Min_%_of_Block_per_department',
    'Unknown1', # Assuming these are the extra columns
    'Unknown2'
]
department_split_data = department_split_data[['Department_Sub-Department', 'Splittable', 'Min_%_of_Block_per_department']].copy()

department_split_data.columns = department_split_data.columns.str.strip()
print(department_split_data.columns.tolist())
# Build dictionaries:
dept_splittable = department_split_data.set_index('Department_Sub-Department')['Splittable'].to_dict()
dept_min_pct    = department_split_data.set_index('Department_Sub-Department')['Min_%_of_Block_per_department'].to_dict()

# 1.4 Min%Split sheet (not used below but loaded)
min_split_data = pd.read_excel(
    excel_path,
    sheet_name='Min % Split'
)
min_split_data.columns = min_split_data.columns.str.strip()

# 1.5 Adjacency sheet
xls = pd.ExcelFile(excel_path)
adjacency_sheet_name = [name for name in xls.sheet_names if "Adjacency" in name][0]
raw_data = xls.parse(adjacency_sheet_name, header=1, index_col=0)
adjacency_data = raw_data.apply(pd.to_numeric, errors='coerce')
adjacency_data.index   = adjacency_data.index.str.strip()
adjacency_data.columns = adjacency_data.columns.str.strip()

# 1.6 De-Centralized Logic sheet
df_logic = pd.read_excel(
    excel_path,
    sheet_name='De-Centralized Logic',
    header=None
)
De_Centralized_data = {}
current_section = None
for _, row in df_logic.iterrows():
    first_cell = str(row[0]).strip() if pd.notna(row[0]) else ""
    if first_cell in ["Centralised", "Semi Centralized", "DeCentralised"]:
        current_section = first_cell
        De_Centralized_data[current_section] = {"Add": 0}
    elif current_section and first_cell == "( Add into cetralised destination Block)":
        De_Centralized_data[current_section]["Add"] = int(row[1]) if pd.notna(row[1]) else 0

for key in ["Centralised", "Semi Centralized", "DeCentralised"]:
    if key not in De_Centralized_data:
        De_Centralized_data[key] = {"Add": 0}

# ----------------------------------------
# Step 2: Preprocess Blocks & Department Split
# ----------------------------------------

# 2.2 Separate Destination vs. Typical blocks
destination_blocks = all_block_data[
    all_block_data['Typical_Destination'] == 'Destination'
].copy()
typical_blocks = all_block_data[
    all_block_data['Typical_Destination'] == 'Typical'
].copy()

# ----------------------------------------
# Step 3: Initialize Floor Assignments
# ----------------------------------------

def initialize_floor_assignments(floor_df):
    """
    Returns a dict keyed by floor name. Each entry has:
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
            'remaining_area': row['Usable_Area_(SQM)'],
            'remaining_capacity': row['Max_Assignable_Floor_loading_Capacity'], # Fixed: removed .iloc[0]
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
# Step 4: Core Stacking Function (with updated department splitting logic)
# ----------------------------------------

def run_stack_plan(mode):
    """
    mode: 'centralized', 'semi', or 'decentralized'
    Returns four DataFrames:
      detailed_df      – block‐to‐floor assignments
      floor_summary_df – floor totals (count, area, occupancy)
      space_mix_df     – for each floor & category {ME, WE, US, Support, Speciality}:
                          Unit_Count_on_Floor,
                          Pct_of_Floor_UC,
                          Pct_of_Overall_UC
      unassigned_df    – blocks that couldn't be placed
    """
    assignments = initialize_floor_assignments(all_floor_data)
    unassigned_blocks = []

    # 4.1 Determine how many floors to use for destination blocks
    def destination_floor_count():
        if mode == 'centralized':
            return 2
        elif mode == 'semi':
            return 2 + De_Centralized_data["Semi Centralized"]["Add"]
        elif mode == 'decentralized':
            return 2 + De_Centralized_data["DeCentralised"]["Add"]
        else:
            return 2

    max_dest_floors = min(destination_floor_count(), len(floors))

    # 4.2 Group destination blocks by Destination_Group
    dest_groups = {}
    for _, blk in destination_blocks.iterrows():
        grp = blk['Destination_Group']
        if grp not in dest_groups:
            dest_groups[grp] = {'blocks': [], 'total_area': 0.0, 'total_capacity': 0}
        dest_groups[grp]['blocks'].append(blk.to_dict())
        dest_groups[grp]['total_area'] += blk['Cumulative_Area_SQM']
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
                blocks_sorted = sorted(info_grp['blocks'], key=lambda b: b['Cumulative_Area_SQM'], reverse=True)
                removed_blocks = []
                trial_blocks = blocks_sorted.copy()

                while trial_blocks:
                    trial_area = sum(b['Cumulative_Area_SQM'] for b in trial_blocks)
                    trial_capacity = sum(b['Max_Occupancy_with_Capacity'] for b in trial_blocks)

                    # Try to place this reduced group
                    floor_combination = []
                    temp_assignments = {f: assignments[f].copy() for f in floors}
                    temp_floors_by_space = sorted(floors, key=lambda f: assignments[f]['remaining_area'], reverse=True)

                    temp_success = True
                    for blk in trial_blocks:
                        blk_area = blk['Cumulative_Area_SQM']
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
                            assignments[fl]['remaining_area'] -= blk['Cumulative_Area_SQM']
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
                            assignments[fl]['remaining_capacity'] >= blk_capacity):
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

    # ----------------------------------------
    # Phase 2: Handle typical blocks with UPDATED department‐splittable logic
    # ----------------------------------------

    # 4.3 Categorize typical blocks by department splittable values
    # Categories:
    # - not_splittable: splittable values 1 and 0.75
    # - order_by_area: splittable value 0 (assign in descending order of remaining floor area)
    # - fully_splittable: splittable value -1 (can be split across all floors)

    def add_space_mix_area(floor, block, area):
        """Helper function to add area to appropriate space mix category"""
        cat = block['SpaceMix_(ME_WE_US_Support_Speciality)'].strip()
        if cat == 'ME':
            assignments[floor]['ME_area'] += area
        elif cat == 'WE':
            assignments[floor]['WE_area'] += area
        elif cat == 'US':
            assignments[floor]['US_area'] += area
        elif cat.lower() == 'support':
            assignments[floor]['Support_area'] += area
        elif cat.lower() == 'speciality':
            assignments[floor]['Speciality_area'] += area

    not_splittable_groups = {}  # dept -> [blocks] for splittable in [1, 0.75]
    order_by_area_groups = {}   # dept -> [blocks] for splittable = 0
    fully_splittable_blocks = []  # individual blocks for splittable = -1

    for blk in typical_blocks.to_dict('records'):
        dept = blk['Department_Sub_Department'].strip()
        # Default to -1 (fully splittable) if missing
        spl = dept_splittable.get(dept, -1)

        if spl in [1, 0.75]:
            # Not splittable - keep department together
            not_splittable_groups.setdefault(dept, []).append(blk)
        elif spl == 0:
            # Assign in descending order of remaining floor area
            order_by_area_groups.setdefault(dept, []).append(blk)
        else:  # spl == -1 or any other value
            # Fully splittable across all floors
            fully_splittable_blocks.append(blk)

    # 4.4 Phase 2A: Assign not_splittable departments (splittable = 1 or 0.75)
    print(f"Processing {len(not_splittable_groups)} not-splittable departments...")
    for dept, blocks_list in not_splittable_groups.items():
        total_area = sum(b['Cumulative_Area_SQM'] for b in blocks_list)
        total_cap  = sum(b['Max_Occupancy_with_Capacity'] for b in blocks_list)
        placed = False

        # Try to place entire department on one floor
        candidate_floors = sorted(
            floors,
            key=lambda f: assignments[f]['remaining_area'],
            reverse=True
        )
        for fl in candidate_floors:
            if (assignments[fl]['remaining_area'] >= total_area and
                assignments[fl]['remaining_capacity'] >= total_cap):
                for blk in blocks_list:
                    assignments[fl]['assigned_blocks'].append(blk)
                    assignments[fl]['assigned_departments'].add(dept)
                    add_space_mix_area(fl, blk, blk['Cumulative_Area_SQM'])
                assignments[fl]['remaining_area'] -= total_area
                assignments[fl]['remaining_capacity'] -= total_cap
                placed = True
                print(f"  Placed dept {dept} on floor {fl}")
                break

        if not placed:
            # Mark entire department group as unassigned
            unassigned_blocks.extend(blocks_list)
            print(f"  Could not place dept {dept} - added to unassigned")

    # 4.5 Phase 2B: Assign order_by_area departments (splittable = 0)
    print(f"Processing {len(order_by_area_groups)} order-by-area departments...")
    for dept, blocks_list in order_by_area_groups.items():
        total_area = sum(b['Cumulative_Area_SQM'] for b in blocks_list)
        total_cap  = sum(b['Max_Occupancy_with_Capacity'] for b in blocks_list)

        # First try to place entire department on one floor
        placed_whole = False
        floors_by_remaining_area = sorted(
            floors,
            key=lambda f: assignments[f]['remaining_area'],
            reverse=True
        )

        for fl in floors_by_remaining_area:
            if (assignments[fl]['remaining_area'] >= total_area and
                assignments[fl]['remaining_capacity'] >= total_cap):
                for blk in blocks_list:
                    assignments[fl]['assigned_blocks'].append(blk)
                    assignments[fl]['assigned_departments'].add(dept)
                    add_space_mix_area(fl, blk, blk['Cumulative_Area_SQM'])
                assignments[fl]['remaining_area'] -= total_area
                assignments[fl]['remaining_capacity'] -= total_cap
                placed_whole = True
                print(f"  Placed dept {dept} entirely on floor {fl}")
                break

        # If can't place whole department, place blocks individually in descending order of floor area
        if not placed_whole:
            print(f"  Splitting dept {dept} across floors...")
            for blk in blocks_list:
                blk_area = blk['Cumulative_Area_SQM']
                blk_capacity = blk['Max_Occupancy_with_Capacity']
                placed_block = False

                # Get current floor order by remaining area (descending)
                floors_by_remaining_area = sorted(
                    floors,
                    key=lambda f: assignments[f]['remaining_area'],
                    reverse=True
                )

                for fl in floors_by_remaining_area:
                    if (assignments[fl]['remaining_area'] >= blk_area and
                        assignments[fl]['remaining_capacity'] >= blk_capacity):
                        assignments[fl]['assigned_blocks'].append(blk)
                        assignments[fl]['assigned_departments'].add(dept)
                        add_space_mix_area(fl, blk, blk_area)
                        assignments[fl]['remaining_area'] -= blk_area
                        assignments[fl]['remaining_capacity'] -= blk_capacity
                        placed_block = True
                        break

                if not placed_block:
                    unassigned_blocks.append(blk)

    # 4.6 Phase 2C: Assign fully_splittable blocks (splittable = -1) using space-mix proportional logic
    print(f"Processing {len(fully_splittable_blocks)} fully-splittable blocks...")

    # 4.6.a Assign all ME blocks randomly first
    me_blocks = [
        blk for blk in fully_splittable_blocks
        if blk['SpaceMix_(ME_WE_US_Support_Speciality)'].strip() == 'ME'
    ]
    random.shuffle(me_blocks)
    for blk in me_blocks:
        blk_area     = blk['Cumulative_Area_SQM']
        blk_capacity = blk['Max_Occupancy_with_Capacity']
        blk_dept     = blk['Department_Sub_Department'].strip()

        candidate_floors = floors.copy()
        random.shuffle(candidate_floors)
        placed = False
        for fl in candidate_floors:
            if (assignments[fl]['remaining_area'] >= blk_area and
                assignments[fl]['remaining_capacity'] >= blk_capacity):
                assignments[fl]['assigned_blocks'].append(blk)
                assignments[fl]['remaining_area'] -= blk_area
                assignments[fl]['remaining_capacity'] -= blk_capacity
                assignments[fl]['assigned_departments'].add(blk_dept)
                assignments[fl]['ME_area'] += blk_area
                placed = True
                break
        if not placed:
            unassigned_blocks.append(blk)

    # 4.6.b Compute ME distribution per floor (unit counts)
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
        me_frac_per_floor = {
            fl: me_count_per_floor[fl] / total_me for fl in floors
        }

    # 4.6.c Assign other categories proportionally
    other_categories = ['WE', 'US', 'Support', 'Speciality']
    for category in other_categories:
        cat_blocks = [
            blk for blk in fully_splittable_blocks
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
            blk_area     = blk['Cumulative_Area_SQM']
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
                if (assignments[fl]['remaining_area'] >= blk_area and
                    assignments[fl]['remaining_capacity'] >= blk_capacity):
                    assignments[fl]['assigned_blocks'].append(blk)
                    assignments[fl]['remaining_area'] -= blk_area
                    assignments[fl]['remaining_capacity'] -= blk_capacity
                    assignments[fl]['assigned_departments'].add(blk_dept)
                    add_space_mix_area(fl, blk, blk_area)
                    assigned_counts[fl] += 1
                    placed = True
                    break

            if not placed:
                # Try fallback random floors
                fallback = floors.copy()
                random.shuffle(fallback)
                for fl in fallback:
                    if (assignments[fl]['remaining_area'] >= blk_area and
                        assignments[fl]['remaining_capacity'] >= blk_capacity):
                        assignments[fl]['assigned_blocks'].append(blk)
                        assignments[fl]['remaining_area'] -= blk_area
                        assignments[fl]['remaining_capacity'] -= blk_capacity
                        assignments[fl]['assigned_departments'].add(blk_dept)
                        add_space_mix_area(fl, blk, blk_area)
                        assigned_counts[fl] += 1
                        placed = True
                        break

            if not placed:
                unassigned_blocks.append(blk)

    # Final pass: try to place any still‐unassigned blocks
    for blk in unassigned_blocks.copy():
        ba = blk['Cumulative_Area_SQM']
        bc = blk['Max_Occupancy_with_Capacity']
        for fl in floors:
            if (assignments[fl]['remaining_area'] >= ba and
                assignments[fl]['remaining_capacity'] >= bc):
                assignments[fl]['assigned_blocks'].append(blk)
                assignments[fl]['assigned_departments'].add(
                    blk['Department_Sub_Department'].strip()
                )
                assignments[fl]['remaining_area'] -= ba
                assignments[fl]['remaining_capacity'] -= bc
                unassigned_blocks.remove(blk)
                break

    # 4.6 Phase 3: Build Output DataFrames

    # 4.6.1 Detailed DataFrame
    assignment_list = []
    for fl, info in assignments.items():
        for blk in info['assigned_blocks']:
            assignment_list.append({
                'Floor': fl,
                'Department': blk['Department_Sub_Department'],
                'Block_Name': blk['Block_Name'],
                'Destination_Group': blk['Destination_Group'],
                'SpaceMix': blk['SpaceMix_(ME_WE_US_Support_Speciality)'],
                'Assigned_Area_SQM': blk['Cumulative_Area_SQM'],
                'Max_Occupancy': blk['Max_Occupancy_with_Capacity']
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
    'Name', 'Usable_Area_(SQM)', 'Max_Assignable_Floor_loading_Capacity'
]].rename(columns={
    'Name': 'Floor',
    'Usable_Area_(SQM)': 'Input_Usable_Area_SQM',
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

    # 3.3 “SpaceMix_By_Units” DataFrame
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
            # Percent of floor’s blocks
            pct_of_floor = (cnt / total_blocks_on_floor * 100) if total_blocks_on_floor else 0.0
            # Percent of overall blocks of that category
            total_cat = category_totals[cat]
            pct_overall = (cnt / total_cat * 100) if total_cat else 0.0

            rows.append({
                'Floor': fl,
                'SpaceMix': cat,
                '%spaceMix': round(pct_overall, 2)

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
            'Max_Occupancy': blk.get('Max_Occupancy_with_Capacity', 0)
        })
    unassigned_df = pd.DataFrame(unassigned_list)

    return detailed_df, floor_summary_df, space_mix_df, unassigned_df



# ----------------------------------------
# Step 5: Generate & Export Excel + CSV Files (including Unassigned)
# ----------------------------------------

# ----------------------------------------
# Step 5: Run & Export (with sampling to minimize unassigned)
# ----------------------------------------

def best_plan(mode, trials=50):
    best = None
    best_unassigned = float('inf')
    for seed in range(trials):
        random.seed(seed)
        det, fs, sm, un = run_stack_plan(mode)
        if len(un) < best_unassigned:
            best_unassigned = len(un)
            best = (det, fs, sm, un)
    return best

central_detailed, central_floor_sum, central_space_mix, central_unassigned = best_plan('centralized', trials=50)
semi_detailed,    semi_floor_sum,    semi_space_mix,    semi_unassigned    = best_plan('semi', trials=50)
decentral_detailed, decentral_floor_sum, decentral_space_mix, decentral_unassigned = best_plan('decentralized', trials=50)


# File names
central_file    = 'stack_plan_centralized28.xlsx'
semi_file       = 'stack_plan_semi_centralized28.xlsx'
decentral_file  = 'stack_plan_decentralized28.xlsx'

# --- ExcelWriter blocks with an extra sheet "Unassigned" ---
with pd.ExcelWriter(central_file) as writer:
    central_detailed.to_excel(writer, sheet_name='Detailed', index=False)
    central_floor_sum.to_excel(writer, sheet_name='Floor_Summary', index=False)
    central_space_mix.to_excel(writer, sheet_name='SpaceMix_By_Units', index=False)
    central_unassigned.to_excel(writer, sheet_name='Unassigned', index=False)

with pd.ExcelWriter(semi_file) as writer:
    semi_detailed.to_excel(writer, sheet_name='Detailed', index=False)
    semi_floor_sum.to_excel(writer, sheet_name='Floor_Summary', index=False)
    semi_space_mix.to_excel(writer, sheet_name='SpaceMix_By_Units', index=False)
    semi_unassigned.to_excel(writer, sheet_name='Unassigned', index=False)

with pd.ExcelWriter(decentral_file) as writer:
    decentral_detailed.to_excel(writer, sheet_name='Detailed', index=False)
    decentral_floor_sum.to_excel(writer, sheet_name='Floor_Summary', index=False)
    decentral_space_mix.to_excel(writer, sheet_name='SpaceMix_By_Units', index=False)
    decentral_unassigned.to_excel(writer, sheet_name='Unassigned', index=False)

print("✅ Generated three Excel outputs (each with an 'Unassigned' sheet):")
print(f"    • {central_file}")
print(f"    • {semi_file}")
print(f"    • {decentral_file}")