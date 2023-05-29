# build docker img

- run `docker build . -t pydemo:latest -f pydemo.dockerfile`

# demo.sql

a raw file which should work by just run each sql in sql-clinet

- make sure jar files are prepared by runing cmd at the top
- run `docker-compose -f flink-session.yml run sql-client`

# run demo with easy-sql in session-mode

- change different-version files you want to run in `flink-session.yml -> demo -> command`
- run `docker-compose -f flink-session.yml run demo`

# run demo in application-mode with k8s-operator

- install ref https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/try-flink-kubernetes-operator/quick-start/#deploying-the-operator
- run `kubectl apply -f flink-application.yml`
- check logs `kubectl logs -f deploy/pyjob`

# prepare pg db

- run `python prepare_db.py`
