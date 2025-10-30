from pydantic import BaseModel, Field, field_validator
from typing import Optional


class SPARQLValue(BaseModel):
    """
    Represents a single value in SPARQL JSON response

    Example:
        {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q5'}
        {'type': 'literal', 'value': 'human', 'xml:lang': 'en'}
    """
    type: str = Field(..., description="Value type: uri, literal")
    value: str = Field(..., description="The actual value")
    xml_lang: Optional[str] = Field(None, alias="xml:lang", description="Language tag for literals")
    datatype: Optional[str] = Field(None, description="Datatype for typed literals")

    class Config:
        populate_by_name = True  # Allows both 'xml_lang' and 'xml:lang'


class SPARQLResult(BaseModel):
    """
    Represents one result row from Wikidata SPARQL query

    Corresponds to query:
        SELECT ?event1 ?event1Label ?event2 ?event2Label ?causeType
    """
    event1: SPARQLValue = Field(..., description="Cause entity URI")
    event1Label: SPARQLValue = Field(..., description="Cause entity label")
    event2: SPARQLValue = Field(..., description="Effect entity URI")
    event2Label: SPARQLValue = Field(..., description="Effect entity label")
    causeType: SPARQLValue = Field(..., description="Type of causation")

    @field_validator('event1', 'event2')
    @classmethod
    def validate_uri_type(cls, v: SPARQLValue) -> SPARQLValue:
        """Ensure event URIs are actually URIs"""
        if v.type != 'uri':
            raise ValueError(f"Expected type 'uri', got '{v.type}'")
        return v

    @field_validator('causeType')
    @classmethod
    def validate_cause_type(cls, v: SPARQLValue) -> SPARQLValue:
        """Ensure causeType is a literal"""
        if v.type != 'literal':
            raise ValueError(f"Expected type 'literal', got '{v.type}'")
        return v
