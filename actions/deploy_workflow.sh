docker tag fast_km-worker:build rmillikin/fast_km-worker:dev
docker tag fast_km-server:build rmillikin/fast_km-server:dev
docker push rmillikin/fast_km-worker:dev
docker push rmillikin/fast_km-server:dev