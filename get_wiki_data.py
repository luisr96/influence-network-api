import os
import csv
import time
from datetime import datetime
from dotenv import load_dotenv

from SPARQLWrapper import SPARQLWrapper, JSON

from models import SPARQLResult
from typing import Dict, List
from pydantic import ValidationError

load_dotenv()

user_agent = os.getenv("USER_AGENT")
endpoint_url = "https://query.wikidata.org/sparql"

def fetch_batch(limit=1, offset=0, max_retries=5) -> List[dict]:
    """ From Wikidata Query Service Query Builder:
    P828 = "has cause"
    P1478 = "immediate cause of"
    """
    query = f"""SELECT ?event1 ?event1Label ?event2 ?event2Label ?causeType WHERE {{
              {{
                ?event2 wdt:P828 ?event1.
                BIND("has cause" AS ?causeType)
              }} UNION {{
                ?event2 wdt:P1478 ?event1.
                BIND("immediate cause of" AS ?causeType)
              }}

                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
              }}
                LIMIT {limit}
                OFFSET {offset}"""
    for attempt in range(max_retries):
        try:
            sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            return sparql.query().convert()["results"]["bindings"]

        except Exception as e:
            if attempt < max_retries - 1:
                # Exponential backoff
                wait_time = 10 * (2 ** attempt)
                print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                print(f"Waiting {wait_time} seconds before retry")
                time.sleep(wait_time)
            else:
                print(f"All {max_retries} attempts failed")
                raise

def get_id_from_uri(uri: str) -> str:
    """Extract Q-ID from full Wikidata URI
    http://www.wikidata.org/entity/Q12345 -> Q12345
    """
    return uri.split("/")[-1]

def process_data(batch: List[dict],
                nodes: Dict[str, str],
                relationships: List[dict]) -> tuple[Dict[str, str], List[dict]]:
    """
    Process and validate a batch of SPARQL results

    Args:
        batch: Raw SPARQL results
        nodes: Dictionary mapping Q-IDs to labels (strings)
        relationships: List of relationship dictionaries

    Returns:
        Updated nodes and relationships
    """
    for i, item in enumerate(batch):
        try:
            # Does validation
            sparql_result = SPARQLResult(**item)

            event1_id = get_id_from_uri(sparql_result.event1.value)
            event1_label = sparql_result.event1Label.value

            event2_id = get_id_from_uri(sparql_result.event2.value)
            event2_label = sparql_result.event2Label.value

            cause_type = sparql_result.causeType.value

            if event1_id not in nodes:
                nodes[event1_id] = event1_label

            if event2_id not in nodes:
                nodes[event2_id] = event2_label

            relationships.append({
                "cause": event1_id,
                "effect": event2_id,
                "type": cause_type
            })

        except ValidationError as e:
            print(f"Validation error at index {i}:")
            print(f"Error: {e}")
            continue
        except Exception as e:
            print(f"Unexpected error at index {i}: {e}")
            continue

    return nodes, relationships

def write_checkpoint_csv(nodes: Dict[str, str], relationships: List[dict], checkpoint_num: int) -> None:
    """Write checkpoint files in case of error during long runs"""
    nodes_filename = f"checkpoint_nodes_{checkpoint_num}.csv"
    with open(nodes_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id:ID', 'label', ':LABEL'])
        for node_id, label in nodes.items():
            writer.writerow([node_id, label, 'Entity'])

    rels_filename = f"checkpoint_relationships_{checkpoint_num}.csv"
    with open(rels_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([':START_ID', ':END_ID', 'type', ':TYPE'])
        for rel in relationships:
            writer.writerow([rel['cause'], rel['effect'], rel['type'], 'CAUSES'])

    print(f"Checkpoint saved: {nodes_filename}, {rels_filename}")

def write_final_csv(nodes: Dict[str, str], relationships: List[dict]) -> None:
    """Write final CSV files to import into Neo4j"""
    with open('nodes.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id:ID', 'label', ':LABEL'])
        for node_id, label in nodes.items():
            writer.writerow([node_id, label, 'Entity'])
    print(f"Wrote nodes.csv ({len(nodes)} nodes)")

    with open('relationships.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([':START_ID', ':END_ID', 'type', ':TYPE'])
        for rel in relationships:
            writer.writerow([rel['cause'], rel['effect'], rel['type'], 'CAUSES'])
    print(f"Wrote relationships.csv ({len(relationships)} relationships)")

def load_checkpoint(checkpoint_num: int) -> tuple[Dict[str, str], List[dict]]:
    """
    Load nodes and relationships from checkpoint file

    Args:
        checkpoint_num: The checkpoint number e.g. 30000

    Returns:
        Tuple of (nodes dict, relationships list)
    """
    nodes: Dict[str, str] = {}
    relationships: List[dict] = []

    # Load nodes
    nodes_file = f'checkpoint_nodes_{checkpoint_num}.csv'
    print(f"Loading {nodes_file}...")
    with open(nodes_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            nodes[row['id:ID']] = row['label']

    # Load relationships
    rels_file = f'checkpoint_relationships_{checkpoint_num}.csv'
    print(f"Loading {rels_file}...")
    with open(rels_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            relationships.append({
                'cause': row[':START_ID'],
                'effect': row[':END_ID'],
                'type': row['type']
            })

    print(f"Loaded {len(nodes)} nodes and {len(relationships)} relationships from checkpoint")
    return nodes, relationships

def print_nodes_and_relationships(nodes: Dict[str, str], relationships: List[dict], first_n: int =5) -> None:
    """Print summary of nodes and relationships to verify structure"""
    print(f"Unique entities: {len(nodes)}")
    print(f"Total relationships: {len(relationships)}")
    print(f"\nFirst {first_n} nodes:")
    for i, (node_id, label) in enumerate(nodes.items()):
        if i >= first_n:
            break
        print(f"* {node_id}: {label}")

    print(f"\nFirst {first_n} relationships:")
    for i, rel in enumerate(relationships[:first_n]):
        print(f"* ({rel['cause']}) -[{rel['type']}]-> ({rel['effect']})")

def main():

    resume_from_checkpoint = 38500  # Set to None to start from beginning

    if resume_from_checkpoint:
        nodes, relationships = load_checkpoint(resume_from_checkpoint)
        offset = resume_from_checkpoint
    else:
        nodes: Dict[str, str] = {}
        relationships: List[dict] = []
        offset = 0

    batch_size = 100
    checkpoint_frequency = 10000
    delay_between_requests = 5 # Not too often so that Wikidata's servers are happy
    max_results = None  # Set to None for no limit

    batch_num = offset // batch_size

    while True:
        batch_num += 1
        print(f"Batch: {batch_num}, Offset: {offset}")

        try:
            data = fetch_batch(limit=batch_size, offset=offset)
        except Exception as e:
            print(f"Fatal error after all retries: {e}")
            print("Saving current progress before stopping")
            write_checkpoint_csv(nodes, relationships, len(relationships))
            break

        # Check if we got data
        if not data:
            print("No more data available")
            break

        print(f"{len(data)} results")

        # Process the batch
        nodes, relationships = process_data(data, nodes, relationships)
        print(f"Running total: {len(nodes)} entities, {len(relationships)} relationships")

        # Save checkpoint if needed
        if len(relationships) % checkpoint_frequency == 0 and len(relationships) > 0:
            write_checkpoint_csv(nodes, relationships, len(relationships))

        # Check if we've reached our max
        if max_results and len(relationships) >= max_results:
            print(f"Reached maximum of {max_results} relationships")
            break

        # Move to next batch
        offset += batch_size

        print(f"Waiting {delay_between_requests} seconds")
        time.sleep(delay_between_requests)

    # Write final CSV files when successful
    write_final_csv(nodes, relationships)

    print("Summary:")
    print_nodes_and_relationships(nodes, relationships)

if __name__ == "__main__":
    main()
