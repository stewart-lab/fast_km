import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--pw_hash', default='none')
args = parser.parse_args()

def main():
    print('server waiting 10 sec for redis to set up...')
    time.sleep(10)

    import server.app as app
    app.start_server(args.pw_hash)

if __name__ == '__main__':
    main()