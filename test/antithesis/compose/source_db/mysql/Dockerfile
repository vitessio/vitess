FROM mysql:8.4@sha256:da906917ca4ace3ba55538b7c2ee97a9bc865ef14a4b6920b021f0249d603f3d
COPY commerce.sql grant.sh dataset.csv /docker-entrypoint-initdb.d/
COPY mysql84.cnf /etc/mysql/conf.d/mysql84.cnf
