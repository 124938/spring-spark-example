# spring-spark-example
This repo contains spring with spark example

### Start oracle locally using docker
```
docker pull gvenzl/oracle-xe:18.4.0-slim
docker run -d -p 1521:1521 -e ORACLE_PASSWORD=Test@123 -v oracle-volume:/home/asus/tech_soft/oracle_docker_vol/oracle_xe_18_4_0_slim gvenzl/oracle-xe:18.4.0-slim

-- Admin user
SID
user : system
pwd : Test@123

-- Create user
docker exec 24e3f72dab5c createAppUser docker_app_user docker_app_pwd
XEPDB1
docker_app_user
docker_app_pwd

```
