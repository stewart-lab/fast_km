import argparse
import server.app as app

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--pw_hash', default='none')
args = parser.parse_args()

def main():
    app.start_server(args.pw_hash)

if __name__ == '__main__':
    main()