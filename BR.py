import pandas as pd
import random
import math
import PyPDF2
import re

# ----------------------------------------
# Step 1: Load Input Sheets
# ----------------------------------------

excel_path = '/content/B- R2.xlsx'  # adjust if needed

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

# 1.4 Adjacency sheet (original)
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
# Step 2: Physical Constraint Adjacency Logic
# ----------------------------------------

def define_physical_constraints():
    """
    Define physical constraints based on the PDF document
    Returns a dictionary mapping constraint types to their floor priorities
    """
    physical_constraints = {
        'Main Entry within Client Real estate Reception': {
            'priority_1': 'lowest',  # Level 0 if available
            'priority_2': 'mid',
            'priority_3': 'top_most'
        },
        'Transfer floor': {
            'blocks': []  # Nil - no specific blocks
        },
        'Floor with an Outdoor Terrace': {
            'blocks': []  # Nil - no specific blocks
        },
        'Best View': {
            'blocks': []  # Nil - no specific blocks
        },
        'Top most floor of Atrium Floor 2': {
            'blocks': []  # Specific to Floor 2
        },
        'Top Most Level': {
            'priority_1': 'highest'
        },
        'Refuge Floor': {
            'blocks': []  # Nil - no specific blocks
        },
        'Additional Structural loading floor': {
            'blocks': []  # Nil - no specific blocks
        },
        'Loading Dock': {
            'blocks': []  # Nil - no specific blocks
        },
        'Service Floor': {
            'blocks': []  # Nil - no specific blocks
        }
    }
    return physical_constraints

def get_floor_levels(floor_df):
    """
    Determine floor levels based on floor names/numbers
    Returns a dictionary mapping floor names to their level types
    """
    floor_levels = {}
    floors = floor_df['Name'].str.strip().tolist()

    # Sort floors to identify lowest, highest, mid
    # Assuming floors are named with numbers or can be sorted
    sorted_floors = sorted(floors)

    if len(sorted_floors) >= 1:
        floor_levels[sorted_floors[0]] = 'lowest'
        floor_levels[sorted_floors[-1]] = 'highest'

        # Identify mid floors
        if len(sorted_floors) > 2:
            mid_floors = sorted_floors[1:-1]
            for floor in mid_floors:
                floor_levels[floor] = 'mid'
        elif len(sorted_floors) == 2:
            floor_levels[sorted_floors[1]] = 'mid'

    # Special handling for specific floors mentioned in constraints
    for floor in floors:
        if 'atrium' in floor.lower() or '2' in floor:
            floor_levels[floor] = 'atrium_top'

    return floor_levels

def assign_physical_constraint_blocks(block_data, floor_data, physical_constraints):
    """
    Assign blocks based on physical constraints before other assignments
    """
    blocks_df = block_data.copy()
    floor_levels = get_floor_levels(floor_data)

    # Add physical constraint assignment column
    blocks_df['Physical_Constraint_Assignment'] = ''
    blocks_df['Physical_Priority'] = 0

    # Process blocks that have specific physical requirements
    # For now, we'll identify blocks that should follow physical constraints
    # This can be expanded based on specific block naming patterns or additional data

    # Example: Reception blocks should go to lowest floor
    reception_blocks = blocks_df[
        blocks_df['Block_Name'].str.contains('Reception', case=False, na=False)
    ]

    for idx in reception_blocks.index:
        blocks_df.loc[idx, 'Physical_Constraint_Assignment'] = 'Main Entry within Client Real estate Reception'
        blocks_df.loc[idx, 'Physical_Priority'] = 1

    # Example: Executive or VIP blocks should go to highest floors
    executive_blocks = blocks_df[
        blocks_df['Block_Name'].str.contains('Executive|VIP|CEO|Director', case=False, na=False)
    ]

    for idx in executive_blocks.index:
        blocks_df.loc[idx, 'Physical_Constraint_Assignment'] = 'Top Most Level'
        blocks_df.loc[idx, 'Physical_Priority'] = 1

    # Example: Blocks with specific floor requirements
    # You can add more specific logic based on your block naming conventions

    return blocks_df, floor_levels

# ----------------------------------------
# Step 3: Read Adjacency Rules from PDF Files
# ----------------------------------------

def read_pdf_adjacency_rules(pdf_path):
    """Read adjacency rules from PDF file"""
    adjacency_rules = {}

    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text()

        # Parse the text to extract adjacency rules
        lines = text.split('\n')
        current_dept = None
        current_block = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Look for department_subdepartment pattern
            if '_' in line and any(keyword in line for keyword in ['Common', 'External']):
                parts = line.split()
                if len(parts) >= 2:
                    dept_sub = parts[0]
                    block_name = ' '.join(parts[1:])
                    current_dept = dept_sub
                    current_block = block_name

                    if current_dept not in adjacency_rules:
                        adjacency_rules[current_dept] = {}
                    if current_block not in adjacency_rules[current_dept]:
                        adjacency_rules[current_dept][current_block] = {}

            # Look for priority values (1, 0.3, 0)
            elif current_dept and current_block:
                numbers = re.findall(r'\b(?:1|0\.3|0)\b', line)
                if numbers:
                    # Store priority values
                    adjacency_rules[current_dept][current_block]['priorities'] = [float(n) for n in numbers]

    except Exception as e:
        print(f"Error reading PDF {pdf_path}: {e}")

    return adjacency_rules

# Read adjacency rules from both PDF files
adjacency_rules_1 = read_pdf_adjacency_rules('Auto Stacking Input New Build - Case Study A- R1 (with block instances) - Split priority-destination grouping.pdf')
adjacency_rules_2 = read_pdf_adjacency_rules('Auto Stacking Input New Build - Case Study A- R1 (with block instances) - Adjacency-destination grouping.pdf')

# Combine adjacency rules
combined_adjacency_rules = {}
for rules in [adjacency_rules_1, adjacency_rules_2]:
    for dept, blocks in rules.items():
        if dept not in combined_adjacency_rules:
            combined_adjacency_rules[dept] = {}
        combined_adjacency_rules[dept].update(blocks)

# ----------------------------------------
# Step 4: Create Destination Groups Based on Adjacency Rules
# ----------------------------------------

def create_adjacency_based_destination_groups(block_data, adjacency_rules):
    """
    Create destination groups based on adjacency rules instead of block information
    """
    # Create a copy of block data to work with
    blocks_df = block_data.copy()

    # Initialize Destination_Group and Adjacency_Priority columns for all blocks
    blocks_df['Destination_Group'] = None
    blocks_df['Adjacency_Priority'] = None

    # Initialize destination group counter
    group_counter = 1

    # Dictionary to store formed groups
    destination_groups = {}

    # Process blocks marked as destination or both
    destination_blocks = blocks_df[blocks_df['Typical_Destination'].isin(['Destination', 'both'])].copy()

    # Create adjacency-based groups
    for dept_sub, dept_rules in adjacency_rules.items():
        # Find blocks belonging to this department
        dept_blocks = destination_blocks[
            destination_blocks['Department_Sub_Department'].str.strip() == dept_sub
        ].copy()

        if dept_blocks.empty:
            continue

        # Group blocks based on adjacency rules and priorities
        for block_name, rule_info in dept_rules.items():
            # Find blocks with this block name
            matching_blocks = dept_blocks[
                dept_blocks['Block_Name'].str.strip() == block_name
            ].copy()

            if matching_blocks.empty:
                continue

            # Get priority for this block type
            priorities = rule_info.get('priorities', [0])
            max_priority = max(priorities) if priorities else 0

            # Create group name based on department and priority
            if max_priority >= 1.0:
                group_name = f"High_Priority_Group_{group_counter}"
            elif max_priority >= 0.3:
                group_name = f"Medium_Priority_Group_{group_counter}"
            else:
                group_name = f"Low_Priority_Group_{group_counter}"

            # Assign all matching blocks to this group
            for idx in matching_blocks.index:
                blocks_df.loc[idx, 'Destination_Group'] = group_name
                blocks_df.loc[idx, 'Adjacency_Priority'] = max_priority

            # Store group information
            if group_name not in destination_groups:
                destination_groups[group_name] = {
                    'blocks': [],
                    'department': dept_sub,
                    'priority': max_priority,
                    'total_area': 0,
                    'total_capacity': 0
                }

            for _, block in matching_blocks.iterrows():
                destination_groups[group_name]['blocks'].append(block.to_dict())
                destination_groups[group_name]['total_area'] += block['Cumulative_Block_Circulation_Area']
                destination_groups[group_name]['total_capacity'] += block['Max_Occupancy_with_Capacity']

            group_counter += 1

    # Handle any remaining destination blocks that weren't matched
    unmatched_dest_blocks = destination_blocks[
        ~destination_blocks.index.isin(blocks_df[blocks_df['Destination_Group'].notna()].index)
    ]

    if not unmatched_dest_blocks.empty:
        # Group unmatched blocks by department
        for dept in unmatched_dest_blocks['Department_Sub_Department'].unique():
            dept_unmatched = unmatched_dest_blocks[
                unmatched_dest_blocks['Department_Sub_Department'] == dept
            ]

            group_name = f"Unmatched_Dest_Group_{group_counter}"

            for idx in dept_unmatched.index:
                blocks_df.loc[idx, 'Destination_Group'] = group_name
                blocks_df.loc[idx, 'Adjacency_Priority'] = 0

            destination_groups[group_name] = {
                'blocks': dept_unmatched.to_dict('records'),
                'department': dept,
                'priority': 0,
                'total_area': dept_unmatched['Cumulative_Block_Circulation_Area'].sum(),
                'total_capacity': dept_unmatched['Max_Occupancy_with_Capacity'].sum()
            }

            group_counter += 1

    return blocks_df, destination_groups

# ----------------------------------------
# Step 5: Apply Physical Constraints and Adjacency-Based Grouping
# ----------------------------------------

# Apply physical constraints first
physical_constraints = define_physical_constraints()
all_block_data, floor_levels = assign_physical_constraint_blocks(
    all_block_data, all_floor_data, physical_constraints
)

# Apply adjacency-based grouping
all_block_data, adjacency_destination_groups = create_adjacency_based_destination_groups(
    all_block_data, combined_adjacency_rules
)

# ----------------------------------------
# Step 6: Preprocess Blocks & Department Split
# ----------------------------------------

# 6.1 Separate Destination vs. Typical blocks (now with adjacency-based groups)
destination_blocks = all_block_data[all_block_data['Typical_Destination'].isin(['Destination', 'both'])].copy()
typical_blocks = all_block_data[all_block_data['Typical_Destination'] == 'Typical'].copy()

# 6.2 Add priority information to destination blocks
destination_blocks['Priority'] = destination_blocks.get('Adjacency_Priority', 0)

# ----------------------------------------
# Step 7: Initialize Floor Assignments
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
            'remaining_area': row['Usable Area'], # Corrected column name
            'remaining_capacity': row['Max Assignable Floor loading Capacity'], # Corrected column name
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
# Step 8: Enhanced Assignment Functions
# ----------------------------------------

def assign_physical_constraint_blocks_to_floors(assignments, block_data, floor_levels):
    """
    Assign blocks with physical constraints to appropriate floors first
    """
    # Get blocks with physical constraints
    constraint_blocks = block_data[
        block_data['Physical_Constraint_Assignment'] != ''
    ].copy()

    assigned_blocks = []

    for _, block in constraint_blocks.iterrows():
        constraint_type = block['Physical_Constraint_Assignment']
        area = block['Cumulative_Block_Circulation_Area']
        capacity = block['Max_Occupancy_with_Capacity']

        # Determine target floor based on constraint
        target_floors = []

        if constraint_type == 'Main Entry within Client Real estate Reception':
            # Priority 1: lowest, Priority 2: mid, Priority 3: top_most
            target_floors = [
                floor for floor, level in floor_levels.items()
                if level == 'lowest'
            ]
            if not target_floors:
                target_floors = [
                    floor for floor, level in floor_levels.items()
                    if level == 'mid'
                ]
            if not target_floors:
                target_floors = [
                    floor for floor, level in floor_levels.items()
                    if level == 'highest'
                ]

        elif constraint_type == 'Top Most Level':
            target_floors = [
                floor for floor, level in floor_levels.items()
                if level == 'highest'
            ]

        # Try to assign to target floors
        assigned = False
        for floor in target_floors:
            if floor in assignments:
                if (assignments[floor]['remaining_area'] >= area and
                    assignments[floor]['remaining_capacity'] >= capacity):

                    assignments[floor]['assigned_blocks'].append(block.to_dict())
                    assignments[floor]['assigned_departments'].add(
                        block['Department_Sub_Department'].strip()
                    )
                    assignments[floor]['remaining_area'] -= area
                    assignments[floor]['remaining_capacity'] -= capacity
                    assigned_blocks.append(block.name)  # Track assigned block index
                    assigned = True
                    break

        if not assigned:
            print(f"Warning: Could not assign block {block['Block_Name']} with constraint {constraint_type}")

    return assignments, assigned_blocks

def can_groups_be_adjacent(group1_info, group2_info):
    """Check if two groups can be adjacent based on adjacency rules and priorities"""
    # High priority groups (1.0) can be adjacent to any group
    if group1_info['priority'] >= 1.0 or group2_info['priority'] >= 1.0:
        return True

    # Medium priority groups (0.3) can be adjacent to medium and high priority groups
    if (group1_info['priority'] >= 0.3 and group2_info['priority'] >= 0.3):
        return True

    # Same department groups can be adjacent
    if group1_info['department'] == group2_info['department']:
        return True

    return False

def split_destination_groups_by_adjacency(destination_groups):
    """Split destination groups based on adjacency rules and priorities"""
    # Sort groups by priority (highest first)
    sorted_groups = sorted(
        destination_groups.items(),
        key=lambda x: x[1]['priority'],
        reverse=True
    )

    subgroups = []
    current_subgroup = []

    for group_name, group_info in sorted_groups:
        if not current_subgroup:
            current_subgroup.append((group_name, group_info))
        else:
            # Check if this group can be adjacent to any group in current subgroup
            can_group = False
            for existing_group_name, existing_group_info in current_subgroup:
                if can_groups_be_adjacent(group_info, existing_group_info):
                    can_group = True
                    break

            if can_group:
                current_subgroup.append((group_name, group_info))
            else:
                # Start new subgroup
                if current_subgroup:
                    subgroups.append(current_subgroup)
                current_subgroup = [(group_name, group_info)]

    # Add the last subgroup
    if current_subgroup:
        subgroups.append(current_subgroup)

    return subgroups

# ----------------------------------------
# Step 9: Core Stacking Function with Physical Constraints
# ----------------------------------------

def run_stack_plan(mode, priority_category='ME'):
    """
    mode: 'centralized', 'semi', or 'decentralized'
    priority_category: 'ME', 'WE', 'US', or 'Support' - which category to prioritize in typical block assignment
    Returns four DataFrames:
      1) detailed_df      – each block's assigned floor, department, block name, destination group, space mix, area, occupancy
      2) floor_summary_df – floor‐wise totals (block count, total area, total occupancy)
      3) space_mix_df     – for each floor and each category {ME, WE, US, Support, Speciality}
      4) unassigned_df    – blocks that couldn't be placed
    """
    assignments = initialize_floor_assignments(all_floor_data)
    unassigned_blocks = []

    # Phase 0: Assign Physical Constraint Blocks First
    print(f"Phase 0: Assigning physical constraint blocks...")
    assignments, assigned_constraint_blocks = assign_physical_constraint_blocks_to_floors(
        assignments, all_block_data, floor_levels
    )

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

    # Phase 1: Adjacency-Based Destination Group Assignment
    print(f"Phase 1: Assigning destination groups...")

    # Filter out already assigned blocks from destination groups
    filtered_destination_groups = {}
    for group_name, group_info in adjacency_destination_groups.items():
        filtered_blocks = []
        for block in group_info['blocks']:
            # Check if block was already assigned in Phase 0
            if block.get('Block_ID') not in assigned_constraint_blocks:
                filtered_blocks.append(block)

        if filtered_blocks:
            filtered_destination_groups[group_name] = {
                'blocks': filtered_blocks,
                'department': group_info['department'],
                'priority': group_info['priority'],
                'total_area': sum(b['Cumulative_Block_Circulation_Area'] for b in filtered_blocks),
                'total_capacity': sum(b['Max_Occupancy_with_Capacity'] for b in filtered_blocks)
            }

    group_names = list(filtered_destination_groups.keys())
    random.shuffle(group_names)

    for grp_name in group_names:
        grp_info = filtered_destination_groups[grp_name]
        grp_area = grp_info['total_area']
        grp_cap = grp_info['total_capacity']
        placed_whole = False

        # Try to place entire group first on designated destination floors
        candidate_floors = floors[:max_dest_floors].copy()

        for fl in candidate_floors:
            if (assignments[fl]['remaining_area'] >= grp_area and
                assignments[fl]['remaining_capacity'] >= grp_cap):
                # Entire group fits here—place all blocks
                for blk in grp_info['blocks']:
                    assignments[fl]['assigned_blocks'].append(blk)
                    assignments[fl]['assigned_departments'].add(
                        blk['Department_Sub_Department']
                    )
                assignments[fl]['remaining_area'] -= grp_area
                assignments[fl]['remaining_capacity'] -= grp_cap
                placed_whole = True
                break

        # If not placed as whole, try remaining floors
        if not placed_whole:
            for fl in floors[max_dest_floors:]:
                if (assignments[fl]['remaining_area'] >= grp_area and
                    assignments[fl]['remaining_capacity'] >= grp_cap):
                    for blk in grp_info['blocks']:
                        assignments[fl]['assigned_blocks'].append(blk)
                        assignments[fl]['assigned_departments'].add(
                            blk['Department_Sub_Department'].strip()
                        )
                    assignments[fl]['remaining_area'] -= grp_area
                    assignments[fl]['remaining_capacity'] -= grp_cap
                    placed_whole = True
                    break

        # If still not placed, try splitting based on adjacency
        if not placed_whole:
            # Split this group's blocks if possible
            subgroups = split_destination_groups_by_adjacency({grp_name: grp_info})

            for subgroup in subgroups:
                subgroup_area = sum(group_info['total_area'] for _, group_info in subgroup)
                subgroup_cap = sum(group_info['total_capacity'] for _, group_info in subgroup)
                subgroup_blocks = []
                for _, group_info in subgroup:
                    subgroup_blocks.extend(group_info['blocks'])

                subgroup_placed = False

                # Try to place subgroup on available floors
                for fl in floors:
                    if (assignments[fl]['remaining_area'] >= subgroup_area and
                        assignments[fl]['remaining_capacity'] >= subgroup_cap):
                        for blk in subgroup_blocks:
                            assignments[fl]['assigned_blocks'].append(blk)
                            assignments[fl]['assigned_departments'].add(
                                blk['Department_Sub_Department'].strip()
                            )
                        assignments[fl]['remaining_area'] -= subgroup_area
                        assignments[fl]['remaining_capacity'] -= subgroup_cap
                        subgroup_placed = True
                        break

                # If subgroup still can't be placed, add to unassigned
                if not subgroup_placed:
                    for blk in subgroup_blocks:
                        unassigned_blocks.append(blk)


    # Phase 2: Category-prioritized distribution of typical blocks across floors
    print(f"Phase 2: Assigning typical blocks with {priority_category} priority...")

    # Filter out already assigned typical blocks
    remaining_typical_blocks = typical_blocks[
        ~typical_blocks.index.isin(assigned_constraint_blocks)
    ].copy()

    # 2.1 Group typical blocks by SpaceMix category and Block_Name
    typical_recs = remaining_typical_blocks.to_dict('records')

    # Define category order based on priority_category
    all_categories = ['ME', 'WE', 'US', 'Support', 'Speciality']
    if priority_category in all_categories:
        category_order = [priority_category] + [cat for cat in all_categories if cat != priority_category]
    else:
        category_order = all_categories

    # Group blocks by category and then by block name
    category_blocks = {}
    for cat in category_order:
        category_blocks[cat] = {}
        cat_blocks = remaining_typical_blocks[
            remaining_typical_blocks['SpaceMix_(ME_WE_US_Support_Speciality)'].str.strip() == cat
        ]
        for _, blk in cat_blocks.iterrows():
            name = blk['Block_Name']
            if name not in category_blocks[cat]:
                category_blocks[cat][name] = []
            category_blocks[cat][name].append(blk.to_dict())

    # 2.2 Process categories in priority order
    for cat in category_order:
        if cat not in category_blocks:
            continue

        # Compute each floor's available area for this category
        avail = {fl: assignments[fl]['remaining_area'] for fl in floors}
        total_avail = sum(avail.values())

        if total_avail <= 0:
            # No more space available, add remaining blocks to unassigned
            for btype, blks in category_blocks[cat].items():
                for blk in blks:
                    unassigned_blocks.append(blk)
            continue

        # 2.3 For each block type in this category, compute target counts per floor
        for btype, blks in category_blocks[cat].items():
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
                'Block_id': blk.get('Block_ID', ''),
                'Floor': fl,
                'Department': blk.get('Department_Sub_Department', ''),
                'Block_Name': blk.get('Block_Name', ''),
                'Destination_Group': blk.get('Destination_Group', ''),
                'SpaceMix': blk.get('SpaceMix_(ME_WE_US_Support_Speciality)', ''),
                'Assigned_Area_SQM': blk.get('Cumulative_Block_Circulation_Area', 0),
                'Max_Occupancy': blk.get('Max_Occupancy_with_Capacity', 0),
                'Priority': blk.get('Priority', 0),
                'Adjacency_Priority': blk.get('Adjacency_Priority', 0)
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
        'Name', 'Usable Area', 'Max Assignable Floor loading Capacity' # Corrected column name
    ]].rename(columns={
        'Name': 'Floor',
        'Usable Area': 'Input_Usable_Area', # Corrected column name
        'Max Assignable Floor loading Capacity': 'Input_Max_Capacity' # Corrected column name
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
            cat = blk.get('SpaceMix_(ME_WE_US_Support_Speciality)', '').strip()
            if cat in counts:
                counts[cat] += 1
        total_blocks_on_floor = sum(counts.values())

        for cat in all_categories:
            cnt = counts[cat]
            pct_of_floor = (cnt / total_blocks_on_floor * 100) if total_blocks_on_floor else 0.0
            total_cat = category_totals.get(cat, 0) # Use .get with default 0
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
            'Max_Occupancy': blk.get('Max_Occupancy_with_Capacity', 0),
            'Priority': blk.get('Priority', 0),
            'Adjacency_Priority': blk.get('Adjacency_Priority', 0)
        })
    unassigned_df = pd.DataFrame(unassigned_list)

    return detailed_df, floor_summary_df, space_mix_df, unassigned_df

# ----------------------------------------
# Step 8: Generate & Export Files for All Modes and Categories
# ----------------------------------------

# Print adjacency-based destination groups summary
print("📋 Adjacency-Based Destination Groups Summary:")
print("=" * 60)
for group_name, group_info in adjacency_destination_groups.items():
    print(f"\n{group_name}:")
    print(f"  • Department: {group_info.get('department', 'N/A')}") # Use .get
    print(f"  • Priority: {group_info.get('priority', 'N/A')}")     # Use .get
    print(f"  • Total Area: {group_info.get('total_area', 0):.2f} SQM") # Use .get
    print(f"  • Total Capacity: {group_info.get('total_capacity', 0)}") # Use .get
    print(f"  • Number of Blocks: {len(group_info.get('blocks', []))}") # Use .get


# Define categories for priority assignment
priority_categories = ['ME', 'WE', 'US', 'Support']
modes = ['centralized', 'semi', 'decentralized']

# Generate plans for each mode and category combination
all_plans = {}

for mode in modes:
    all_plans[mode] = {}
    for category in priority_categories:
        print(f"\nGenerating {mode} plan with {category} priority...")
        detailed, floor_sum, space_mix, unassigned = run_stack_plan(mode, category)
        all_plans[mode][category] = {
            'detailed': detailed,
            'floor_summary': floor_sum,
            'space_mix': space_mix,
            'unassigned': unassigned
        }

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

# Export to Excel files for each mode and category
for mode in modes:
    for category in priority_categories:
        plan_data = all_plans[mode][category]

        # Create summary
        summary = make_typical_summary(plan_data['detailed'])

        # Export to Excel
        filename = f'stack_plan_{mode}_{category}_priority_adjacency_based.xlsx'
        with pd.ExcelWriter(filename) as writer:
            plan_data['detailed'].to_excel(writer, sheet_name='Detailed', index=False)
            plan_data['floor_summary'].to_excel(writer, sheet_name='Floor_Summary', index=False)
            plan_data['space_mix'].to_excel(writer, sheet_name='SpaceMix_By_Units', index=False)
            plan_data['unassigned'].to_excel(writer, sheet_name='Unassigned', index=False)
            if not summary.empty:
                summary.to_excel(writer, sheet_name='Typical_Summary')

print("\n✅ Generated Excel outputs for all modes and priority categories.")