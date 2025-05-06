import os
import glob
import json
import csv


def flatten_json(json_obj, parent_key='', sep='_'):
    """
    Recursively flattens a nested JSON object.
    Args:
        json_obj (dict): JSON object to flatten.
        parent_key (str): Base key string for recursion.
        sep (str): Separator for flattened keys.

    Returns:
        dict: Flattened JSON object.
    """
    items = []
    for k, v in json_obj.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Convert list to string for CSV compatibility
            items.append((new_key, ','.join(map(str, v))))
        else:
            items.append((new_key, v))
    return dict(items)


def convert_json_to_csv(json_file, output_dir):
    """
    Converts a JSON file to a CSV file.
    Args:
        json_file (str): Path to the JSON file.
        output_dir (str): Directory to save the CSV file.
    """
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Flatten JSON data
    if isinstance(data, list):  # If the JSON is an array of objects
        flattened_data = [flatten_json(item) for item in data]
    else:  # If the JSON is a single object
        flattened_data = [flatten_json(data)]

    # Determine CSV file path
    base_name = os.path.basename(json_file)
    csv_file = os.path.join(output_dir, f"{os.path.splitext(base_name)[0]}.csv")

    # Write to CSV
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=flattened_data[0].keys())
        writer.writeheader()
        writer.writerows(flattened_data)

    print(f"Converted {json_file} to {csv_file}")


def find_and_convert_json(data_dir, output_dir):
    """
    Finds all JSON files in a directory and converts them to CSV files.
    Args:
        data_dir (str): Path to the data directory.
        output_dir (str): Directory to save the CSV files.
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Find all JSON files
    json_files = glob.glob(os.path.join(data_dir, '**', '*.json'), recursive=True)
    print(f"Found {len(json_files)} JSON files.")

    # Convert each JSON file to CSV
    for json_file in json_files:
        convert_json_to_csv(json_file, output_dir)


if __name__ == "__main__":
    # Define input and output directories
    data_directory = os.path.join(os.getcwd(), 'data')
    output_directory = os.path.join(os.getcwd(), 'output')

    # Find JSON files and convert to CSV
    find_and_convert_json(data_directory, output_directory)