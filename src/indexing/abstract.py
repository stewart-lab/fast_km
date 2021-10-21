class Abstract():
    def __init__(self, pmid: int, year: int, title: str, text: str):
        self.pmid = pmid
        self.pub_year = year
        self.title = title
        self.text = text