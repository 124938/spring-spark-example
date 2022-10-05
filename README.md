# spring-spark-example
This repo contains spring with spark example

### Start oracle locally using docker
```
sus@Asus:~/tech_soft/oracle_docker_vol$ docker pull gvenzl/oracle-xe:18.4.0-slim

sus@Asus:~/tech_soft/oracle_docker_vol$ docker images
REPOSITORY         TAG           IMAGE ID       CREATED         SIZE
gvenzl/oracle-xe   18.4.0-slim   42ae566dd3da   4 weeks ago     1.94GB

sus@Asus:~/tech_soft/oracle_docker_vol$ docker run -d -p 1521:1521 -e ORACLE_PASSWORD=Test@123 -v oracle-volume:/home/asus/tech_soft/oracle_docker_vol/oracle_xe_18_4_0_slim gvenzl/oracle-xe:18.4.0-slim
24e3f72dab5ca856094712a65ee1499393ea94f59f1c21ab67f5f3f13bec0d59

asus@Asus:~/tech_soft/oracle_docker_vol$ docker ps -a
CONTAINER ID   IMAGE                          COMMAND                  CREATED          STATUS                      PORTS                                       NAMES
24e3f72dab5c   gvenzl/oracle-xe:18.4.0-slim   "container-entrypoin…"   24 minutes ago   Up 24 minutes               0.0.0.0:1521->1521/tcp, :::1521->1521/tcp   inspiring_brahmagupta

-- Admin user
SID
user : system
pwd : Test@123
```
