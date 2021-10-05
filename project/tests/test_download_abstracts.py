from project.src import download_abstracts
import unittest

class TestDownloadPubMedAbstracts(unittest.TestCase):
    def test_connect_to_ftp_server(self):
        #ftp = DownloadPubMedAbstracts.connect_to_ftp_server(
        #    'ftp.ncbi.nlm.nih.gov', 'pubmed/baseline/')

        #items = ftp.dir()

        #ftp.quit()

        self.assertTrue(False)

if __name__ == '__main__':
    unittest.main()