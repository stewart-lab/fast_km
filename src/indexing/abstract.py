class Abstract():
    def __init__(self, pmid: int, year: int, title: str, text: str):
        self.pmid = pmid
        self.pub_year = year
        self.title = title
        self.text = text

        if not self.title or str.isspace(self.title):
            self.title = ' '
        if not self.text or str.isspace(self.text):
            self.text = ' '