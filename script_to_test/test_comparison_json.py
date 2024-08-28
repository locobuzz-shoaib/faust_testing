def extract_ids(file_path):
    ids = set()
    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()
            if '"_id"' in line:
                # Extract the ID value
                start_idx = line.find('"', line.find(":") + 1) + 1
                end_idx = line.find('"', start_idx)
                doc_id = line[start_idx:end_idx]
                ids.add(doc_id)
    print(len(ids))
    return ids


def compare_json_files(file1, file2):
    # Extract document IDs from both files
    doc_ids_1 = extract_ids(file1)
    doc_ids_2 = extract_ids(file2)

    # Find missing documents in each JSON
    missing_in_file1 = doc_ids_2 - doc_ids_1
    missing_in_file2 = doc_ids_1 - doc_ids_2

    return missing_in_file1, missing_in_file2


# Paths to your JSON files
file1_path = "sc.json"
file2_path = "sc1.json"

missing_in_file1, missing_in_file2 = compare_json_files(file1_path, file2_path)

if not missing_in_file1 and not missing_in_file2:
    print("Both JSON files contain the same documents.")
else:
    if missing_in_file1:
        print("Documents missing in first file:", missing_in_file1)
    if missing_in_file2:
        print("Documents missing in second file:", missing_in_file2)
