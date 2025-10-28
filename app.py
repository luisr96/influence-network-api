from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
from typing import List
from pydantic import BaseModel
import os
import uvicorn
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="Causal Network API",
    description="API for querying causal relationships between entities from Wikidata",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

class Entity(BaseModel):
    id: str
    label: str

def execute_query(query: str, parameters: dict = None):
    with driver.session() as session:
        result = session.run(query, parameters or {})
        return [record.data() for record in result]

@app.on_event("shutdown")
def shutdown_event():
    driver.close()

@app.get("/search", response_model=List[Entity])
def search_entities(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(20, ge=1, le=100)
):
    """Search for entities by label"""
    try:
        query = """
        MATCH (n:Entity)
        WHERE toLower(n.label) CONTAINS toLower($search_term)
        RETURN n.id as id, n.label as label
        ORDER BY n.label
        LIMIT $limit
        """
        results = execute_query(query, {"search_term": q, "limit": limit})

        return [Entity(id=result["id"], label=result["label"]) for result in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
