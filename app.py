from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import os
import uvicorn
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="Influence Network API",
    description="API for exploring influence relationships between entities",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "https://influence-network-ui.vercel.app",
        "https://*.vercel.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Response models
class EntitySummary(BaseModel):
    id: str
    name: str
    type: str

class EntityDetail(BaseModel):
    id: str
    name: str
    type: str
    properties: Dict[str, Any]

class InfluenceNode(BaseModel):
    id: str
    name: str
    type: str
    properties: Dict[str, Any]

class InfluenceGraph(BaseModel):
    center: InfluenceNode
    influenced_by: List[InfluenceNode]
    influences: List[InfluenceNode]


@app.get("/")
def root():
    return {"message": "Influence Network API", "version": "1.0"}


@app.get("/api/search", response_model=List[EntitySummary])
def search_entities(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(10, ge=1, le=50, description="Maximum results")
):
    """
    Search for entities across all node types by name.
    Returns autocomplete suggestions ranked by relevance and influence.
    """
    query = """
    CALL db.index.fulltext.queryNodes('entity_names', $search_term + '*')
    YIELD node, score
    WITH node, score
    OPTIONAL MATCH (node)-[:INFLUENCED]-()
    WITH node, score, count(*) as degree
    RETURN node.id AS id, node.name AS name, labels(node)[0] AS type,
           (score * 0.3 + degree * 0.7) as combined_score
    ORDER BY combined_score DESC, node.name
    LIMIT $limit
    """

    with driver.session() as session:
        result = session.run(query, search_term=q, limit=limit)
        entities = [
            EntitySummary(
                id=record["id"],
                name=record["name"],
                type=record["type"]
            )
            for record in result
        ]

    return entities


@app.get("/api/entities/{entity_id}", response_model=EntityDetail)
def get_entity_details(entity_id: str):
    """
    Get detailed information about a specific entity.
    """
    query = """
    MATCH (n {id: $entity_id})
    RETURN n.id AS id, n.name AS name, labels(n)[0] AS type, properties(n) AS properties
    """

    with driver.session() as session:
        result = session.run(query, entity_id=entity_id)
        record = result.single()

        if not record:
            raise HTTPException(status_code=404, detail="Entity not found")

        return EntityDetail(
            id=record["id"],
            name=record["name"],
            type=record["type"],
            properties=record["properties"]
        )


@app.get("/api/entities/{entity_id}/influences", response_model=InfluenceGraph)
def get_influence_graph(entity_id: str):
    """
    Get the influence graph for an entity:
    - The entity itself
    - Entities that influenced it (incoming INFLUENCED relationships)
    - Entities it influenced (outgoing INFLUENCED relationships)
    """
    query = """
    MATCH (center {id: $entity_id})

    OPTIONAL MATCH (influencer)-[:INFLUENCED]->(center)
    WITH center, collect(DISTINCT influencer) AS influencers

    OPTIONAL MATCH (center)-[:INFLUENCED]->(influenced)
    WITH center, influencers, collect(DISTINCT influenced) AS influenced_list

    RETURN
        center.id AS center_id,
        center.name AS center_name,
        labels(center)[0] AS center_type,
        properties(center) AS center_properties,
        [n IN influencers | {
            id: n.id,
            name: n.name,
            type: labels(n)[0],
            properties: properties(n)
        }] AS influenced_by,
        [n IN influenced_list | {
            id: n.id,
            name: n.name,
            type: labels(n)[0],
            properties: properties(n)
        }] AS influences
    """

    with driver.session() as session:
        result = session.run(query, entity_id=entity_id)
        record = result.single()

        if not record:
            raise HTTPException(status_code=404, detail="Entity not found")

        return InfluenceGraph(
            center=InfluenceNode(
                id=record["center_id"],
                name=record["center_name"],
                type=record["center_type"],
                properties=record["center_properties"]
            ),
            influenced_by=[
                InfluenceNode(**node) for node in record["influenced_by"]
            ],
            influences=[
                InfluenceNode(**node) for node in record["influences"]
            ]
        )

@app.get("/api/random", response_model=EntitySummary)
def get_random_entity():
    """
    Get a random entity from the database.
    """
    query = """
    MATCH (n)
    WHERE n.name IS NOT NULL
    WITH n, rand() AS random
    ORDER BY random
    LIMIT 1
    RETURN n.id AS id, n.name AS name, labels(n)[0] AS type
    """

    with driver.session() as session:
        result = session.run(query)
        record = result.single()

        if not record:
            raise HTTPException(status_code=404, detail="No entities found")

        return EntitySummary(
            id=record["id"],
            name=record["name"],
            type=record["type"]
        )


@app.on_event("shutdown")
def shutdown_event():
    driver.close()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
