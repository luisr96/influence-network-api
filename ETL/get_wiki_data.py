import os
import time
from dotenv import load_dotenv
import pandas as pd
import re
import json

from SPARQLWrapper import SPARQLWrapper, JSON
from models import SPARQLValue
from typing import List, Optional

load_dotenv()

user_agent = os.getenv("USER_AGENT")
endpoint_url = "https://query.wikidata.org/sparql"

ENTITY_TYPES = {
    "humans": {
        "filename": "humans.csv",
        "label": "Human",
        "wikidata_types": ["wd:Q5"],
        "properties": {
            "birth_date": {"property": "wdt:P569", "label": False},
            "place_of_birth": {"property": "wdt:P19", "label": True},
            "occupation": {"property": "wdt:P106", "label": True},
        }
    },
    "musical_groups": {
        "filename": "musical_groups.csv",
        "label": "MusicalGroup",
        "wikidata_types": [
            "wd:Q215380",    # musical group
            "wd:Q5741069",   # rock band
            "wd:Q25391823",  # punk band
            "wd:Q9212979",   # musical duo
            "wd:Q56816954",  # heavy metal band
            "wd:Q641066",    # girl band
            "wd:Q216337",    # boy band
            "wd:Q18127"      # record label
        ],
        "properties": {
            "inception": {"property": "wdt:P571", "label": False},
            "genre": {"property": "wdt:P136", "label": True},
            "country_of_origin": {"property": "wdt:P495", "label": True},
        }
    },
    "genres": {
        "filename": "genres.csv",
        "label": "Genre",
        "wikidata_types": [
            "wd:Q188451",    # music genre
            "wd:Q1792379",   # art genre
        ],
        "properties": {
            "inception": {"property": "wdt:P571", "label": False},
            "country_of_origin": {"property": "wdt:P495", "label": True},
        }
    },

    "social_political_economic": {
        "filename": "social_political_economic.csv",
        "label": "SocioPoliticalEntity",
        "wikidata_types": [
            "wd:Q41710",     # ethnic group
            "wd:Q16334295",  # group of humans
            "wd:Q264965",    # subculture
            "wd:Q49773",     # social movement
            "wd:Q2198855",   # cultural movement
            "wd:Q12909644",  # political ideology
            "wd:Q7210356",   # political organization
            "wd:Q43229",     # organization
            "wd:Q163740",    # nonprofit organization
            "wd:Q79913",     # non-governmental organization
            "wd:Q155271",    # think tank
            "wd:Q4830453",   # business
            "wd:Q7278",      # political party
            "wd:Q3048444",   # school of economic thought
            "wd:Q17524420",  # aspect of history
            "wd:Q273120",    # protest
        ],
        "properties": {
            "inception": {"property": "wdt:P571", "label": False},
            "country": {"property": "wdt:P17", "label": True},
        }
    },

    "religion_philosophy": {
        "filename": "religion_philosophy.csv",
        "label": "Ideology",
        "wikidata_types": [
            "wd:Q9174",      # religion
            "wd:Q879146",    # Christian denomination
            "wd:Q123129246", # Christian denominational family
            "wd:Q995347",    # Christian movement
            "wd:Q5043",      # Christianity
            "wd:Q19097",     # sect
            "wd:Q13414953",  # religious denomination
            "wd:Q1826286",   # religious movement
            "wd:Q7257",      # ideology
            "wd:Q477544",    # new religious movement
            "wd:Q1530022",   # religious organization
            "wd:Q20643955",  # human biblical figure
            "wd:Q2915955",   # philosophical movement
            "wd:Q12765852",  # philosophical schools and traditions
            "wd:Q1387659",   # school of thought
        ],
        "properties": {
            "inception": {"property": "wdt:P571", "label": False},
        }
    },

    "art": {
        "filename": "art.csv",
        "label": "Art",
        "wikidata_types": [
            "wd:Q3305213",   # painting
            "wd:Q860861",    # sculpture
            "wd:Q93184",     # drawing
            "wd:Q1792644",   # art style
            "wd:Q968159",    # art movement
            "wd:Q2736610",   # artistic school
            "wd:Q25679497",  # art of an area
            "wd:Q1792379",   # art genre
            "wd:Q3326867",   # painting movement
            "wd:Q2198855",   # cultural movement
            "wd:Q667276",    # art exhibition
            "wd:Q12043905",  # pastel painting
            "wd:Q4502119",   # art group
            "wd:Q1400264",   # artist collective
        ],
        "properties": {
            "inception": {"property": "wdt:P571", "label": False},
            "country": {"property": "wdt:P17", "label": True},
        }
    },

    "media": {
        "filename": "media.csv",
        "label": "Media",
        "wikidata_types": [
            "wd:Q11424",     # film
            "wd:Q202866",    # animated film
            "wd:Q201658",    # film genre
            "wd:Q5398426",   # television series
            "wd:Q581714",    # animated series
            "wd:Q117467246", # animated television series
            "wd:Q15416",     # television program
            "wd:Q7889",      # video game
            "wd:Q7058673",   # video game series
            "wd:Q659563",    # video game genre
            "wd:Q1643932",   # tabletop role-playing game
            "wd:Q7777573",   # theatrical genre
            "wd:Q838795",    # comic strip
            "wd:Q21198342",  # manga series
            "wd:Q277759",    # book series
            "wd:Q7725634",   # literary work
            "wd:Q47461344",  # written work
            "wd:Q5185279",   # poem
            "wd:Q223393",    # literary genre
            "wd:Q386724",    # work
            "wd:Q41298",     # magazine
            "wd:Q196600",    # media franchise
        ],
        "properties": {
            "publication_date": {"property": "wdt:P577", "label": False},
            "author": {"property": "wdt:P50", "label": True},
            "director": {"property": "wdt:P57", "label": True},
            "genre": {"property": "wdt:P136", "label": True},
        }
    },

    "language": {
        "filename": "languages.csv",
        "label": "Language",
        "wikidata_types": [
            "wd:Q34770",     # language
            "wd:Q1288568",   # modern language
            "wd:Q33742",     # natural language
            "wd:Q33215",     # constructed language
            "wd:Q1790577",   # planned language
            "wd:Q25295",     # language family
            "wd:Q33384",     # dialect
            "wd:Q1208380",   # dialect group
        ],
        "properties": {
            "inception": {"property": "wdt:P571", "label": False},
            "country": {"property": "wdt:P17", "label": True},
        }
    },
    "cuisine": {
        "filename": "cuisines.csv",
        "label": "Cuisine",
        "wikidata_types": [
            "wd:Q18291645",  # cuisine by ethnic group
            "wd:Q1968435",   # national cuisine
            "wd:Q94951",     # regional cuisine
        ],
        "properties": {
            "inception": {"property": "wdt:P571", "label": False},
            "country": {"property": "wdt:P17", "label": True},
        }
    },
    "technology": {
        "filename": "technology.csv",
        "label": "Technology",
        "wikidata_types": [
            "wd:Q9143",      # programming language
            "wd:Q1144882",   # declarative programming language
            "wd:Q1993334",   # interpreted language
            "wd:Q187432",    # scripting language
            "wd:Q28920142",  # array programming language
            "wd:Q21562092",  # imperative programming language
            "wd:Q28922885",  # procedural programming language
            "wd:Q899523",    # object-based language
            "wd:Q12772052",  # multi-paradigm programming language
            "wd:Q3839507",   # functional programming language
            "wd:Q37045",     # markup language
            "wd:Q845739",    # query language
            "wd:Q65966993",  # hypertext system
            "wd:Q47506",     # compiler
            "wd:Q7397",      # software
            "wd:Q341",       # free software
            "wd:Q1130645",   # open-source software
            "wd:Q14656",     # Unix-like operating system
            "wd:Q9135",      # operating system
            "wd:Q1074158",   # educational software
            "wd:Q132364",    # communication protocol
            "wd:Q15836568",  # computer network protocol
            "wd:Q235557",    # file format
        ],
        "properties": {
            "inception": {"property": "wdt:P571", "label": False},
        }
    },
}


def build_entity_query(entity_config: dict) -> dict:
    """Build SPARQL query configuration from entity config"""

    # Build the type filter as UNION of all wikidata_types
    type_filters = " UNION ".join([
        f"{{ ?entity wdt:P31 {wtype}. }}"
        for wtype in entity_config["wikidata_types"]
    ])

    property_selects = []
    property_optionals = []

    for prop_name, prop_config in entity_config["properties"].items():
        prop_code = prop_config["property"]

        if prop_config["label"]:
            # Property with label
            property_selects.append(f"?{prop_name} ?{prop_name}Label")
            property_optionals.append(f"OPTIONAL {{ ?entity {prop_code} ?{prop_name}. }}")
        else:
            # Property without label, like dates
            property_selects.append(f"?{prop_name}")
            property_optionals.append(f"OPTIONAL {{ ?entity {prop_code} ?{prop_name}. }}")

    properties_select_str = " ".join(property_selects)
    properties_optional_str = "\n              ".join(property_optionals)

    return {
        "properties": properties_select_str,
        "type_filters": type_filters,
        "optionals": properties_optional_str
    }


def fetch_batch_data(query_config: dict, batch_size: int, max_retries: int, entity_type: str) -> List[dict]:
    """Generic batch fetcher"""
    all_results = []
    offset = 0

    while True:
        query = f"""SELECT DISTINCT
              ?entity ?entityLabel
              {query_config['properties']}
            WHERE {{
              {{
                ?entity wdt:P737 ?influencer.
                {query_config['type_filters']}
              }} UNION {{
                ?influenced wdt:P737 ?entity.
                {query_config['type_filters']}
              }}

              {query_config['optionals']}

              SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
            }}
            LIMIT {batch_size}
            OFFSET {offset}"""

        for attempt in range(max_retries):
            try:
                print(f"Fetching {entity_type} batch at offset {offset} (attempt {attempt + 1}/{max_retries})")
                sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                sparql.setTimeout(300)

                try:
                    results = sparql.query().convert()["results"]["bindings"]
                except Exception as e:
                    print(f"Error: {e}, trying alternate parsing")
                    response = sparql.query()
                    raw_json = response.response.read().decode('utf-8', errors='ignore')
                    # Remove control chars and replace problem sequences
                    cleaned_json = re.sub(r'[\x00-\x1f\x7f-\x9f]+', ' ', raw_json)
                    # Remove standalone backslashes that some datapoints seem to have
                    cleaned_json = re.sub(r'\\(?!["\\/bfnrt]|u[0-9a-fA-F]{4})', '', cleaned_json)
                    try:
                        results = json.loads(cleaned_json, strict=False)["results"]["bindings"]
                    except:
                        # If still failing, try lenient parsing
                        print(f"Still failing, using most lenient parsing")
                        results = json.loads(cleaned_json.replace('\n', ' ').replace('\r', ' '), strict=False)["results"]["bindings"]

                if not results:
                    print(f"Finished fetching {entity_type}. Total: {len(all_results)}")
                    return all_results

                all_results.extend(results)
                print(f"Fetched {len(results)} results (total: {len(all_results)})")
                offset += batch_size
                time.sleep(5)
                break

            except Exception as e:
                if "429" in str(e):
                    wait_time = 60
                    print(f"Hit 429 Too Many Requests. Waiting {wait_time} seconds")
                    time.sleep(wait_time)
                    continue

                if attempt < max_retries - 1:
                    wait_time = 10 * (2 ** attempt)
                    print(f"Attempt failed: {e}")
                    print(f"Waiting {wait_time} seconds")
                    time.sleep(wait_time)
                else:
                    print(f"Failed after {max_retries} attempts")
                    return all_results


def get_id_from_uri(uri: str) -> str:
    """Extract Q-ID from full Wikidata URI"""
    return uri.split("/")[-1]


def get_optional_field(item: dict, field_name: str) -> Optional[str]:
    """Safely extract optional field from SPARQL result"""
    if field_name in item and item[field_name]:
        try:
            return SPARQLValue(**item[field_name]).value
        except:
            return None
    return None


def process_entity_data(raw_data: List[dict], entity_config: dict) -> pd.DataFrame:
    """Generic processor for any entity type"""
    nodes = {}

    for item in raw_data:
        try:
            entity = SPARQLValue(**item["entity"])
            entity_label = SPARQLValue(**item["entityLabel"])
            entity_id = get_id_from_uri(entity.value)

            if entity_id not in nodes:
                # Build node dict dynamically based on properties
                node = {
                    "id:ID": entity_id,
                    "name": entity_label.value,
                }

                # Add all configured properties
                for prop_name, prop_config in entity_config["properties"].items():
                    if prop_config["label"]:
                        field_name = f"{prop_name}Label"
                    else:
                        field_name = prop_name

                    node[prop_name] = get_optional_field(item, field_name)

                # Add label
                node[":LABEL"] = entity_config["label"]

                nodes[entity_id] = node

        except Exception as e:
            continue

    return pd.DataFrame(list(nodes.values()))


def fetch_relationships(batch_size=10000, max_retries=3) -> List[dict]:
    """Fetch all influence relationship IDs"""
    all_results = []
    offset = 0

    while True:
        query = f"""SELECT DISTINCT
              ?influenced_entity ?influencer_entity
            WHERE {{
              ?influenced_entity wdt:P737 ?influencer_entity.
            }}
            LIMIT {batch_size}
            OFFSET {offset}"""

        for attempt in range(max_retries):
            try:
                print(f"Fetching relationships batch at offset {offset} (attempt {attempt + 1}/{max_retries})")
                sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                sparql.setTimeout(300)

                try:
                    results = sparql.query().convert()["results"]["bindings"]
                except Exception as json_error:
                    print(f"  JSON parsing failed, trying with cleaning: {json_error}")
                    response = sparql.query()
                    raw_json = response.response.read().decode('utf-8', errors='ignore')
                    cleaned_json = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', raw_json)
                    results = json.loads(cleaned_json)["results"]["bindings"]

                if not results:
                    print(f" Finished fetching relationships. Total: {len(all_results)}")
                    return all_results

                all_results.extend(results)
                print(f"   Fetched {len(results)} results (total: {len(all_results)})")
                offset += batch_size
                time.sleep(5)
                break

            except Exception as e:
                if "429" in str(e):
                    wait_time = 60
                    print(f"  Hit 429 Too Many Requests. Waiting {wait_time} seconds")
                    time.sleep(wait_time)
                    continue

                if attempt < max_retries - 1:
                    wait_time = 10 * (2 ** attempt)
                    print(f"   Attempt failed: {e}")
                    print(f"   Waiting {wait_time} seconds")
                    time.sleep(wait_time)
                else:
                    print(f"   Failed after {max_retries} attempts")
                    return all_results


def process_relationships(raw_data: List[dict]) -> pd.DataFrame:
    """Process relationships into DataFrame"""
    relationships = set()

    for item in raw_data:
        try:
            influenced = SPARQLValue(**item["influenced_entity"])
            influencer = SPARQLValue(**item["influencer_entity"])

            influenced_id = get_id_from_uri(influenced.value)
            influencer_id = get_id_from_uri(influencer.value)

            relationships.add((influencer_id, influenced_id))
        except:
            continue

    return pd.DataFrame([
        {":START_ID": start, ":END_ID": end, ":TYPE": "INFLUENCED"}
        for start, end in relationships
    ])


def main():
    entity_counts = {}

    try:
        # Fetch and process each configured entity type
        for entity_name, entity_config in ENTITY_TYPES.items():
            print(f"Fetching {entity_name}")

            query_config = build_entity_query(entity_config)

            raw_data = fetch_batch_data(query_config, 10000, 3, entity_name)

            df = process_entity_data(raw_data, entity_config)

            filename = entity_config["filename"]
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"Saved {len(df)} {entity_name} to {filename}")

            entity_counts[entity_name] = len(df)

        # Fetch and process relationships
        print("Fetching Relationships")
        rels_data = fetch_relationships()
        rels_df = process_relationships(rels_data)
        rels_df.to_csv('relationships.csv', index=False, encoding='utf-8')
        print(f"Saved {len(rels_df)} relationships to relationships.csv")

        print("Data collection complete")
        for entity_name, count in entity_counts.items():
            print(f"{entity_name}: {count}")

    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
