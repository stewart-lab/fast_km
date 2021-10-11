import os
import math
from ..src.database import Database

def test_database_simple(tmp_path):
    # create the database
    local_path = os.path.join(tmp_path, 'database.sqlite')
    db = Database(local_path)

    # create an abstract
    filename = 'test.xml.gz'
    tokens = ['the', 'quick', 'brown', 'fox', 'jumped', 'over', 'the', 'lazy']
    pmid = 1234567

    # save the abstract to the database
    db.dump_cache(tokens, pmid, [filename])

    # 'brown' is present at position 2
    loc = db.get_word_loc('brown', pmid)
    assert len(loc) == 1
    assert loc[0] == 2

    # 'fox' is present at position 3
    loc = db.get_word_loc('fox', pmid)
    assert len(loc) == 1
    assert loc[0] == 3

    # word is not present in abstract
    loc = db.get_word_loc('test', pmid)
    assert len(loc) == 0

    # PMID does not exist
    loc = db.get_word_loc('fox', (pmid + 1))
    assert len(loc) == 0

    # 1 file has been indexed
    files = db.get_indexed_abstracts_files()
    assert len(files) == 1
    assert files[0] == filename

    result = db.query("the lazy")

    result = db.query("the lazy dog")
    ff = 0