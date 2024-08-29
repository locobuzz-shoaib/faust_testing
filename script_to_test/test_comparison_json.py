import json
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import PatternFill


def extract_social_and_doc_ids(file_path):
    """
    Extracts all unique (socialID, _id) pairs from the given JSON file.

    Args:
        file_path (str): Path to the JSON file.

    Returns:
        list: A list of dictionaries containing 'socialID' and '_id'.
    """
    extracted_data = []

    with open(file_path, "r", encoding='utf-8') as file:
        in_source_section = False
        current_id = None

        for line in file:
            line = line.strip()

            # Extract _id
            if '"_id"' in line:
                start_idx = line.find('"', line.find(":") + 1) + 1
                end_idx = line.find('"', start_idx)
                current_id = line[start_idx:end_idx]

            # Identify the start of the _source section
            if '"_source"' in line:
                in_source_section = True

            # Extract socialID within the _source section
            if in_source_section and '"socialID"' in line:
                start_idx = line.find('"', line.find(":") + 1) + 1
                end_idx = line.find('"', start_idx)
                social_id = line[start_idx:end_idx]

                # Add the (socialID, _id) pair to the list
                if current_id:
                    extracted_data.append({'socialID': social_id, '_id': current_id})

                in_source_section = False  # Reset for the next section

    print(f"Total unique (socialID, _id) pairs found: {len(extracted_data)}")
    return extracted_data


def compare_and_create_excel_report(file1, file2, output_file):
    # Extract (socialID, _id) pairs from both files
    data_file1 = extract_social_and_doc_ids(file1)
    data_file2 = extract_social_and_doc_ids(file2)

    # Convert lists to DataFrames
    df1 = pd.DataFrame(data_file1)
    df2 = pd.DataFrame(data_file2)

    # Merge the DataFrames on both `socialID` and `_id`
    comparison_df = pd.merge(df1, df2, on=['socialID', '_id'], how='outer', indicator=True)

    # Add a column to indicate whether the pair exists in both files or not
    comparison_df['Matched'] = comparison_df['_merge'] == 'both'

    # Rename columns for clarity
    comparison_df.rename(columns={'_id': '_id_file1'}, inplace=True)
    comparison_df['_id_file2'] = comparison_df['_id_file1']  # since we're merging on both columns

    # Create an Excel writer object and write the DataFrame to Excel
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        comparison_df.to_excel(writer, index=False, sheet_name='Comparison')

        # Access the Excel sheet to apply formatting
        worksheet = writer.sheets['Comparison']

        # Define a fill pattern for mismatches
        fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

        # Highlight mismatches in the Excel sheet
        for row in range(2, len(comparison_df) + 2):  # start from the second row because the first row is the header
            if not comparison_df.iloc[row - 2]['Matched']:
                worksheet[f'A{row}'].fill = fill  # socialID
                worksheet[f'B{row}'].fill = fill  # _id_file1
                worksheet[f'C{row}'].fill = fill  # _id_file2

    print(f"Excel report generated: {output_file}")


# Paths to your JSON files and output Excel file
file1_path = "sc1.json"
file2_path = "sc2.json"
output_file = "comparison_report.xlsx"

compare_and_create_excel_report(file1_path, file2_path, output_file)
