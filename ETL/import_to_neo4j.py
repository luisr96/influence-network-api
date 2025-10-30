import pandas as pd
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

script_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(script_dir, 'data')

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def create_constraints():
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Human) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:MusicalGroup) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Genre) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Art) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Media) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Language) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Cuisine) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Technology) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SocioPoliticalEntity) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Ideology) REQUIRE n.id IS UNIQUE",
    ]

    with driver.session() as session:
        for constraint in constraints:
            session.run(constraint)
    print("Constraints created")

def import_nodes(filename):
    filepath = os.path.join(data_dir, filename)
    df = pd.read_csv(filepath)
    label = df[':LABEL'].iloc[0]

    property_columns = [col for col in df.columns if col not in [':LABEL', 'id:ID']]

    print(f"Importing {filename} ({len(df)} nodes)")

    with driver.session() as session:
        for _, row in df.iterrows():
            node_data = {'id': row['id:ID']}
            for col in property_columns:
                if pd.notna(row[col]):
                    node_data[col] = row[col]

            query = f"CREATE (n:{label} $props)"
            session.run(query, props=node_data)

    print(f"Imported {len(df)} {label} nodes")

def import_relationships():
    filepath = os.path.join(data_dir, 'relationships_cleaned.csv')
    df = pd.read_csv(filepath)

    print(f"Importing relationships ({len(df)} total)")

    with driver.session() as session:
        for _, row in df.iterrows():
            query = """
            MATCH (start {id: $start_id})
            MATCH (end {id: $end_id})
            CREATE (start)-[:INFLUENCED]->(end)
            """
            session.run(query, start_id=row[':START_ID'], end_id=row[':END_ID'])

    print(f"Imported {len(df)} relationships")

def clear_database():
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        print("Database cleared")

def get_stats():
    with driver.session() as session:
        node_count = session.run("MATCH (n) RETURN count(n) as count").single()['count']
        rel_count = session.run("MATCH ()-[r]->() RETURN count(r) as count").single()['count']

        print("\n" + "="*60)
        print(f"Total nodes: {node_count:,}")
        print(f"Total relationships: {rel_count:,}")
        print("="*60)

def main():
    # Might want to clear the database before import
    # clear_database()

    create_constraints()

    node_files = [
        'humans_cleaned.csv',
        'musical_groups_cleaned.csv',
        'genres_cleaned.csv',
        'art_cleaned.csv',
        'media_cleaned.csv',
        'languages_cleaned.csv',
        'cuisines_cleaned.csv',
        'technology_cleaned.csv',
        'social_political_economic_cleaned.csv',
        'religion_philosophy_cleaned.csv'
    ]

    for filename in node_files:
        filepath = os.path.join(data_dir, filename)
        if os.path.exists(filepath):
            import_nodes(filename)

    import_relationships()
    get_stats()

    driver.close()
    print("Import complete")

if __name__ == "__main__":
    main()
