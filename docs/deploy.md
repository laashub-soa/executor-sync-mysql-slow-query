```
部署

​```
docker stop executor-sync-mysql-slow-query && docker rm executor-sync-mysql-slow-query


mkdir -p /data/tristan/soa/executor-sync-mysql-slow-query/config && chmod 777 /data/tristan/soa/executor-sync-mysql-slow-query/config

docker run -d --restart=always --name executor-sync-mysql-slow-query \
  -v /etc/timezone:/etc/timezone \
  -v /etc/localtime:/etc/localtime \
  -v /data/tristan/soa/executor-sync-mysql-slow-query/config:/usr/src/app/configs \
  laashubsoa/executor-sync-mysql-slow-query:0.0.2

docker logs -f --tail 100 executor-sync-mysql-slow-query

docker exec -it executor-sync-mysql-slow-query bash
​```

``
```