import pandas as pd
import os

def clean_csvs_for_neo4j(data_dir='data'):
    """Clean all CSV files for Neo4j import and remove entries with Q-ID as name"""

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, data_dir)

    # Check if directory exists
    if not os.path.exists(data_path):
        print(f"Error: Directory '{data_path}' not found")
        print(f"Current script location: {script_dir}")
        print(f"Looking for data in: {data_path}")
        return

    print(f"Cleaning CSVs in: {data_path}")

    # Get all CSV files except relationships
    csv_files = [f for f in os.listdir(data_path) if f.endswith('.csv') and f != 'relationships.csv']

    if not csv_files:
        print("No CSV files found")
        return

    total_removed = 0
    all_valid_ids = set()

    for filename in csv_files:
        filepath = os.path.join(data_path, filename)
        print(f"Cleaning {filename}")
        df = pd.read_csv(filepath)

        original_count = len(df)

        # Remove rows where name is a Q-ID
        if 'name' in df.columns:
            q_id_mask = df['name'].fillna('').str.match(r'^Q\d+$')
            removed_count = q_id_mask.sum()

            if removed_count > 0:
                df = df[~q_id_mask]
                total_removed += removed_count

        # Create new filename with _cleaned suffix
        base_name = os.path.splitext(filename)[0]
        cleaned_filename = f"{base_name}_cleaned.csv"
        cleaned_filepath = os.path.join(data_path, cleaned_filename)

        # Save cleaned version
        df.to_csv(cleaned_filepath, index=False, encoding='utf-8')
        print(f"Removed {original_count - len(df)} rows with Q-ID as name")
        print(f"Saved {len(df)} rows to {cleaned_filename}")

    # Clean relationships
    rels_path = os.path.join(data_path, 'relationships.csv')
    if os.path.exists(rels_path):
        print(f"Cleaning relationships.csv")
        rels_df = pd.read_csv(rels_path)
        original_rel_count = len(rels_df)

        # Remove relationships where either node doesn't exist
        valid_rels = rels_df[
            rels_df[':START_ID'].isin(all_valid_ids) &
            rels_df[':END_ID'].isin(all_valid_ids)
        ]

        orphaned_count = original_rel_count - len(valid_rels)

        cleaned_rels_path = os.path.join(data_path, 'relationships_cleaned.csv')
        valid_rels.to_csv(cleaned_rels_path, index=False, encoding='utf-8')
        print(f"Removed {orphaned_count} orphaned relationships")
        print(f"Saved {len(valid_rels)} relationships to relationships_cleaned.csv")

    print(f"Removed {total_removed} node entries with Q-ID as name")
    print("All files cleaned")

if __name__ == "__main__":
    clean_csvs_for_neo4j()
