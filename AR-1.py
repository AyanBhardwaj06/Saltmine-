import pandas as pd
import random
import math

# ----------------------------------------
# Step 1: Load Input Sheets
# ----------------------------------------

excel_path = '/content/AR--1.xlsx'  # adjust if needed

# 1.1 Floors sheet
all_floor_data = pd.read_excel(excel_path, sheet_name='Program Table Input 2 - Floor')
all_floor_data.columns = all_floor_data.columns.str.strip()

# 1.2 Blocks sheet
all_block_data = pd.read_excel(excel_path, sheet_name='Program Table Input 1 - Block')
all_block_data.columns = all_block_data.columns.str.strip()

# 1.3 Department Split sheet
department_split_data = pd.read_excel(excel_path, sheet_name='Department Split', skiprows=1)
department_split_data.columns = department_split_data.columns.str.strip()
department_split_data = department_split_data.rename(
    columns={'BU_Department_Sub-Department': 'Department_Sub-Department'}
)

# 1.4 Adjacency sheet
xls = pd.ExcelFile(excel_path)
adjacency_sheet_name = [name for name in xls.sheet_names if "Adjacency" in name][0]
raw_data = xls.parse(adjacency_sheet_name, header=1, index_col=0)
adjacency_data = raw_data.apply(pd.to_numeric, errors='coerce')
adjacency_data.index = adjacency_data.index.str.strip()
adjacency_data.columns = adjacency_data.columns.str.strip()

# 1.5 De-Centralized Logic sheet
df_logic = pd.read_excel(excel_path, sheet_name='De-Centralized Logic', header=None)
De_Centralized_data = {}
current_section = None
for _, row in df_logic.iterrows():
    first_cell = str(row[0]).strip() if pd.notna(row[0]) else ""
    if first_cell in ["Centralised", "Semi Centralized", "DeCentralised"]:
        current_section = first_cell
        De_Centralized_data[current_section] = {"Add": 0}
    elif current_section and first_cell == "( Add into cetralised destination Block)":
        De_Centralized_data[current_section]["Add"] = int(row[1]) if pd.notna(row[1]) else 0

# Ensure keys exist
for key in ["Centralised", "Semi Centralized", "DeCentralised"]:
    if key not in De_Centralized_data:
        De_Centralized_data[key] = {"Add": 0}
    elif "Add" not in De_Centralized_data[key]:
        De_Centralized_data[key]["Add"] = 0

# ----------------------------------------
# Step 2: Preprocess Blocks & Department Split
# ----------------------------------------

# 2.1 Separate Destination vs. Typical blocks
destination_blocks = all_block_data[all_block_data['Typical_Destination'].isin(['Destination', 'both'])].copy()
typical_blocks = all_block_data[all_block_data['Typical_Destination'] == 'Typical'].copy()

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

floors = list(all_floor_data['Name'].str.strip())

# ----------------------------------------
# Step 4: Core Stacking Function
# ----------------------------------------

def run_stack_plan(mode):
    """
    mode: 'centralized', 'semi', or 'decentralized'
    Returns four DataFrames:
      1) detailed_df      – each block's assigned floor, department, block name, destination group, space mix, area, occupancy
      2) floor_summary_df – floor‐wise totals (block count, total area, total occupancy)
      3) space_mix_df     – for each floor and each category {ME, WE, US, Support, Speciality}
      4) unassigned_df    – blocks that couldn't be placed
    """
    assignments = initialize_floor_assignments(all_floor_data)
    unassigned_blocks = []

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

    # Phase 1: Assign destination groups
    group_names = list(dest_groups.keys())
    random.shuffle(group_names)
    for grp in group_names:
        info_grp = dest_groups[grp]
        grp_area = info_grp['total_area']
        grp_cap = info_grp['total_capacity']
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

        # If still not placed as a whole, add to unassigned
        if not placed_whole:
            for blk in info_grp['blocks']:
                unassigned_blocks.append(blk)

    # Phase 2: Dynamic, per-block-type distribution of typical blocks across floors
    # 2.1 Group typical blocks by Block_Name
    typical_recs = typical_blocks.to_dict('records')
    types = {}
    for blk in typical_recs:
        name = blk['Block_Name']
        types.setdefault(name, []).append(blk)

    # 2.2 Compute each floor's available area for typical
    avail = {fl: assignments[fl]['remaining_area'] for fl in floors}
    total_avail = sum(avail.values())

    # 2.3 For each block type, compute target counts per floor
    for btype, blks in types.items():
        count = len(blks)
        ratios = {fl: (avail[fl] / total_avail if total_avail > 0 else 1/len(floors))
                  for fl in floors}
        raw = {fl: ratios[fl] * count for fl in floors}
        targ = {fl: int(round(raw[fl])) for fl in floors}

        diff = count - sum(targ.values())
        if diff:
            frac = {fl: raw[fl] - math.floor(raw[fl]) for fl in floors}
            if diff > 0:
                for fl in sorted(floors, key=lambda x: frac[x], reverse=True)[:diff]:
                    targ[fl] += 1
            else:
                for fl in sorted(floors, key=lambda x: frac[x])[: -diff]:
                    targ[fl] -= 1

        random.shuffle(blks)
        idx = 0
        for fl in floors:
            for _ in range(targ[fl]):
                if idx >= count:
                    break
                blk = blks[idx]
                idx += 1
                area = blk['Cumulative_Block_Circulation_Area']
                cap = blk['Max_Occupancy_with_Capacity']
                if (assignments[fl]['remaining_area'] >= area
                    and assignments[fl]['remaining_capacity'] >= cap):
                    assignments[fl]['assigned_blocks'].append(blk)
                    assignments[fl]['assigned_departments'].add(
                        blk['Department_Sub_Department']
                    )
                    assignments[fl]['remaining_area'] -= area
                    assignments[fl]['remaining_capacity'] -= cap
                else:
                    unassigned_blocks.append(blk)

        # any leftovers
        while idx < count:
            unassigned_blocks.append(blks[idx])
            idx += 1

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
                'Max_Occupancy': blk['Max_Occupancy_with_Capacity']
            })
    detailed_df = pd.DataFrame(assignment_list)

    # 3.2 Floor_Summary DataFrame
    if not detailed_df.empty:
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
    else:
        floor_summary_df = pd.DataFrame(columns=['Floor', 'Assgn_Blocks', 'Assgn_Area_SQM', 'Total_Occupancy'])

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

    # 3.4 Unassigned DataFrame
    unassigned_list = []
    for blk in unassigned_blocks:
        unassigned_list.append({
            'Department': blk.get('Department_Sub_Department', ''),
            'Block_Name': blk.get('Block_Name', ''),
            'Destination_Group': blk.get('Destination_Group', ''),
            'SpaceMix': blk.get('SpaceMix_(ME_WE_US_Support_Speciality)', ''),
            'Area_SQM': blk.get('Cumulative_Block_Circulation_Area', 0),
            'Max_Occupancy': blk.get('Max_Occupancy_with_Capacity', 0)
        })
    unassigned_df = pd.DataFrame(unassigned_list)

    return detailed_df, floor_summary_df, space_mix_df, unassigned_df

# ----------------------------------------
# Step 5: Generate & Export Three Excel Files
# ----------------------------------------

# Generate plans
central_detailed, central_floor_sum, central_space_mix, central_unassigned = run_stack_plan('centralized')
semi_detailed, semi_floor_sum, semi_space_mix, semi_unassigned = run_stack_plan('semi')
decentral_detailed, decentral_floor_sum, decentral_space_mix, decentral_unassigned = run_stack_plan('decentralized')

# Build dynamic summary for each plan
def make_typical_summary(detailed_df):
    """Create typical block summary"""
    if detailed_df.empty:
        return pd.DataFrame()

    # Get all typical block types from the original data
    types = typical_blocks['Block_Name'].dropna().str.strip().unique()

    # Filter detailed_df for typical blocks only
    typical_detailed = detailed_df[detailed_df['Block_Name'].isin(types)]

    if typical_detailed.empty:
        return pd.DataFrame()

    # Group by Block_Name and Floor
    df = (typical_detailed
          .groupby(['Block_Name', 'Floor'])
          .size()
          .unstack(fill_value=0))

    df['Total_Assigned'] = df.sum(axis=1)

    # Calculate assignment ratio for each block type
    for block_type in df.index:
        total_blocks_of_type = len(typical_blocks[typical_blocks['Block_Name'].str.strip() == block_type])
        df.loc[block_type, 'Assignment_Ratio'] = round(df.loc[block_type, 'Total_Assigned'] / total_blocks_of_type, 3) if total_blocks_of_type > 0 else 0

    return df

# Create summaries
central_summary = make_typical_summary(central_detailed)
semi_summary = make_typical_summary(semi_detailed)
decentral_summary = make_typical_summary(decentral_detailed)

# Export to Excel files
# Centralized
with pd.ExcelWriter('stack_plan_centralized.xlsx') as writer:
    central_detailed.to_excel(writer, sheet_name='Detailed', index=False)
    central_floor_sum.to_excel(writer, sheet_name='Floor_Summary', index=False)
    central_space_mix.to_excel(writer, sheet_name='SpaceMix_By_Units', index=False)
    central_unassigned.to_excel(writer, sheet_name='Unassigned', index=False)
    central_summary.to_excel(writer, sheet_name='Typical_Summary')

# Semi‐centralized
with pd.ExcelWriter('stack_plan_semi_centralized.xlsx') as writer:
    semi_detailed.to_excel(writer, sheet_name='Detailed', index=False)
    semi_floor_sum.to_excel(writer, sheet_name='Floor_Summary', index=False)
    semi_space_mix.to_excel(writer, sheet_name='SpaceMix_By_Units', index=False)
    semi_unassigned.to_excel(writer, sheet_name='Unassigned', index=False)
    semi_summary.to_excel(writer, sheet_name='Typical_Summary')

# Decentralized
with pd.ExcelWriter('stack_plan_decentralized.xlsx') as writer:
    decentral_detailed.to_excel(writer, sheet_name='Detailed', index=False)
    decentral_floor_sum.to_excel(writer, sheet_name='Floor_Summary', index=False)
    decentral_space_mix.to_excel(writer, sheet_name='SpaceMix_By_Units', index=False)
    decentral_unassigned.to_excel(writer, sheet_name='Unassigned', index=False)
    decentral_summary.to_excel(writer, sheet_name='Typical_Summary')

print("✅ Generated three Excel outputs:")
print("    • stack_plan_centralized.xlsx")
print("    • stack_plan_semi_centralized.xlsx")
print("    • stack_plan_decentralized.xlsx")