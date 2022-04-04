import os
from indexing import km_util as util
from workers import work
from workers import loaded_index as li
from .test_index_building import data_dir

def test_get_controlled_vocab(data_dir):
    li.data_path = data_dir
    assert os.path.exists(data_dir)

    cv_path = util.get_controlled_vocab_folder_path(data_dir)
    assert os.path.exists(cv_path)

    # should be able to retrieve contents of drugs_and_devices.txt
    cv = work.get_controlled_vocab({'file_name': 'drugs_and_devices.txt'})
    assert len(cv) == 9

    # should get an error message when trying to read non-existant file
    cv = work.get_controlled_vocab({'file_name': 'file_does_not_exist.txt'})
    assert len(cv) == 1
    assert ' not found ' in cv[0]