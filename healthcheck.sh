echo healthcheck
#./usr/local/bin/supervisord -c ./fast-km/supervisord/supervisord.conf

# check to see if supervisor is running

# start a worker
rq worker --url redis://redis:6379 --worker-class='demo_worker.DemoWorker' --path /fast-km