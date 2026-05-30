FROM mysql:8.4@sha256:c36050afdca850f23cef85703f84c7531a5ae155a11b5ee1c60acb09937c4084
COPY commerce.sql grant.sh dataset.csv /docker-entrypoint-initdb.d/
COPY mysql84.cnf /etc/mysql/conf.d/mysql84.cnf
