FROM mysql:8.4@sha256:563602a18ffd5be220968e8508d84c9dcd80fbffe69e28af51572db29e3285b2
COPY commerce.sql grant.sh dataset.csv /docker-entrypoint-initdb.d/
COPY mysql84.cnf /etc/mysql/conf.d/mysql84.cnf
