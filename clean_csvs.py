import csv
from typing import Set

def clean_nodes(input_file: str = 'nodes.csv',
                output_file: str = 'nodes_cleaned.csv') -> Set[str]:
    """
    Remove invalid nodes or nodes where label equals the Q-ID (no label)

    Args:
        input_file: Input nodes CSV file
        output_file: Output nodes CSV file

    Returns:
        Set of valid Q-IDs that were kept
    """
    valid_nodes: Set[str] = set()
    removed_count = 0
    kept_count = 0

    print(f"Reading {input_file}")

    with open(input_file, 'r', encoding='utf-8') as f_in:
        with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
            reader = csv.DictReader(f_in)
            writer = csv.writer(f_out)

            # Write header
            writer.writerow(['id:ID', 'label', ':LABEL'])

            for row in reader:
                node_id = row['id:ID']
                label = row['label']

                # Keep only if it's a valid Q-ID
                if not (node_id.startswith('Q') and node_id[1:].isdigit()):
                    continue

                # Keep only if label is different from Q-ID
                if label != node_id:
                    writer.writerow([node_id, label, 'Entity'])
                    valid_nodes.add(node_id)
                    kept_count += 1
                else:
                    removed_count += 1
    return valid_nodes


def clean_relationships(valid_nodes: Set[str],
                       input_file: str = 'relationships.csv',
                       output_file: str = 'relationships_cleaned.csv') -> None:
    """
    Remove relationships that reference deleted nodes

    Args:
        valid_nodes: Set of Q-IDs that still exist
        input_file: Input relationships CSV file
        output_file: Output relationships CSV file
    """
    kept_count = 0
    removed_count = 0

    print(f"Reading {input_file}")

    with open(input_file, 'r', encoding='utf-8') as f_in:
        with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
            reader = csv.DictReader(f_in)
            writer = csv.writer(f_out)

            # Write header
            writer.writerow([':START_ID', ':END_ID', 'type', ':TYPE'])

            for row in reader:
                cause_id = row[':START_ID']
                effect_id = row[':END_ID']
                rel_type = row['type']

                # Keep only if both nodes still exist
                if cause_id in valid_nodes and effect_id in valid_nodes:
                    writer.writerow([cause_id, effect_id, rel_type, 'CAUSES'])
                    kept_count += 1
                else:
                    removed_count += 1

def main():
    # Clean nodes and get list of valid Q-IDs
    valid_nodes = clean_nodes(
        input_file='nodes.csv',
        output_file='nodes_cleaned.csv'
    )

    # Clean relationships using valid Q-IDs
    clean_relationships(
        valid_nodes=valid_nodes,
        input_file='relationships.csv',
        output_file='relationships_cleaned.csv'
    )

if __name__ == "__main__":
    main()
