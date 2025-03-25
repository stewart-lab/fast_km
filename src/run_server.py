import time
import argparse
import indexing.km_util as km_util

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--pw_hash', default='none')
parser.add_argument('--redis_address', default='redis:6379', required=False)
parser.add_argument('--flask_port', default=5000, type=int, required=False)
args = parser.parse_args()

def main():
    km_util.redis_address = args.redis_address
    km_util.flask_port = args.flask_port
    print('INFO: server waiting 10 sec for redis to set up...')
    time.sleep(10)

    import server.app as app
    app.start_server(args.pw_hash)

if __name__ == '__main__':
    main()