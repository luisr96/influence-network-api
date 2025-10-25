import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import os
from dotenv import load_dotenv

load_dotenv()

user_agent = os.getenv("USER_AGENT")
endpoint_url = "https://query.wikidata.org/sparql"

def fetch_batch(limit=500, offset=0):
    """ From Wikidata Query Service Query Builder
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

def main():
    nodes = {}
    relationships = []
    print(fetch_batch())

if __name__ == "__main__":
    main()
