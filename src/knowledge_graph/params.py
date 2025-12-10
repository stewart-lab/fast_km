from pydantic import BaseModel, Field


class RelationshipModel(BaseModel):
    head: str = Field(..., description="(Required) Head entity's name")
    head_type: str = Field(..., description="(Required) Type of the head entity (e.g., 'Gene', 'Disease', etc.)")
    tail: str = Field(..., description="(Required) Tail entity's name")
    tail_type: str = Field(..., description="(Required) Type of the tail entity (e.g., 'Gene', 'Disease', etc.)")
    relation: str = Field(..., description="(Required) Type of relationship")
    evidence: list[int] = Field(..., description="(Required) List of PubMed IDs that provide evidence for this relationship")
    source: str = Field(..., description="(Required) Source of the relationship")

class AddRelationshipsParams(BaseModel):
    relationships: list[RelationshipModel] = Field(..., description="List of relationships to add.")

class GetRelationshipsParams(BaseModel):
    entity1: str = Field(..., description="(Required) Entity name to retrieve relationships for")
    entity2: str = Field(..., description="(Required) Second entity name to retrieve relationships for")