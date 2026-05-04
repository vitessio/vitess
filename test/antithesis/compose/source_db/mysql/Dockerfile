FROM mysql:8.4@sha256:43bd9764df60666fb2ba2cf8217dd17b3d2414c150005d2fdd07a0cd73d3b7f5
COPY commerce.sql grant.sh dataset.csv /docker-entrypoint-initdb.d/
COPY mysql84.cnf /etc/mysql/conf.d/mysql84.cnf
