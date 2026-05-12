FROM mysql:8.4@sha256:21d5c55b7f74a97dcbd281dfbb10356a16b1199da956a63b55493418afe2b940
COPY commerce.sql grant.sh dataset.csv /docker-entrypoint-initdb.d/
COPY mysql84.cnf /etc/mysql/conf.d/mysql84.cnf
