docker run -d --name tutorial -p 5432:5432 -e POSTGRES_PASSWORD=aeFahj5N timescale/timescaledb:latest-pg14

docker stop tutorial
docker start tutorial

docker ps