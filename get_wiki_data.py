import os
from dotenv import load_dotenv

from SPARQLWrapper import SPARQLWrapper, JSON

from models import SPARQLResult
from typing import Dict, List
from pydantic import ValidationError

load_dotenv()

user_agent = os.getenv("USER_AGENT")
endpoint_url = "https://query.wikidata.org/sparql"

def fetch_batch(limit=1, offset=0) -> List[dict]:
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
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()["results"]["bindings"]

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
    nodes: Dict[str, str] = {}
    relationships: List[dict] = []

    data = fetch_batch(limit=10, offset=0)

    nodes, relationships = process_data(data, nodes, relationships)
    print_nodes_and_relationships(nodes, relationships)

if __name__ == "__main__":
    main()
