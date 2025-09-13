from pydantic import BaseModel, Field

class IndexingJobParams(BaseModel):
    id: str | None = Field(None, description="Optional job ID. If not provided, an ID will be generated.")

def validate_params(params: IndexingJobParams) -> None:
    pass