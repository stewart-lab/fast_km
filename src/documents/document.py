class Document():
    def __init__(
            self, 
            pmid: int, 
            pub_year: int | None, 
            title: str | None, 
            abstract: str | None, 
            origin: str | None, 
            body: str | None = None, 
            citation_count: int | None = 0
        ) -> None:

        self.pmid = pmid
        self.pub_year = pub_year
        self.title = title
        self.abstract = abstract
        self.body = body
        self.origin = origin
        self.citation_count = citation_count

    def to_dict(self) -> dict:
        return {
            "pmid": self.pmid,
            "pub_year": self.pub_year,
            "title": self.title,
            "abstract": self.abstract,
            "body": self.body,
            "origin": self.origin,
            "citation_count": self.citation_count
        }