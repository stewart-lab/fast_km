from pydantic import BaseModel, Field
import src.global_vars as gvars
from src.indexing.index import Index
from src.documents.document import Document

# --- Pydantic Models ---
class DocumentModel(BaseModel):
    pmid: int = Field(..., description="(Required) PubMed ID of the document")
    title: str | None = Field(None, description="(Optional) Title of the document")
    abstract: str | None = Field(None, description="(Optional) Abstract text of the document")
    body: str | None = Field(None, description="(Optional) Full text body of the document")
    pub_year: int | None = Field(None, description="(Required for new docs, optional for updates) Publication year")
    citation_count: int | None = Field(None, description="(Optional) Number of citations")
    origin: str | None = Field(None, description="(Optional) Origin/source of the document (usually the filename it came from)")

class AddDocumentsParams(BaseModel):
    documents: list[DocumentModel] = Field(..., description="List of documents to add. To update an existing document, leave any fields not being updated as None.")

class GetDocumentsParams(BaseModel):
    pmids: list[int] = Field(..., description="List of PubMed IDs to retrieve")

class DeleteDocumentsParams(BaseModel):
    pmids: list[int] = Field(..., description="List of PubMed IDs to delete.")
    delete_all: bool = Field(False, description="If true, delete all documents.")

# --- CRUD Operations ---
def add_or_update_corpus_docs(params: AddDocumentsParams):
    documents = [
        Document(
            pmid=doc.pmid,
            pub_year=doc.pub_year,
            title=doc.title,
            abstract=doc.abstract,
            body=doc.body,
            origin=doc.origin,
            citation_count=doc.citation_count,
        ) for doc in params.documents
    ]

    idx = Index(gvars.data_dir)
    idx.add_or_update_documents(documents)
    idx.close()
    return {"status": "finished", "result": f"Added/updated {len(params.documents)} documents."}

def get_corpus_docs(params: GetDocumentsParams):
    idx = Index(gvars.data_dir)
    documents = []
    for pmid in params.pmids:
        document = idx.get_document(pmid)
        if document:
            documents.append(document)
    idx.close()
    return {"status": "finished", "result": documents}

def get_corpus_doc_origins():
    idx = Index(gvars.data_dir)
    origins = list(idx.doc_origins)
    idx.close()
    origins.sort()
    return {"status": "finished", "result": origins}

def delete_corpus_docs(params: DeleteDocumentsParams):
    idx = Index(gvars.data_dir)

    n_deleted = 0
    if params.delete_all:
        n_deleted = idx.delete_all_documents()
    else:
        raise NotImplementedError("Delete not yet implemented.")

    idx.close()
    return {"status": "finished", "result": f"Deleted {n_deleted} documents."}
