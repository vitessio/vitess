FROM mysql:8.4@sha256:77e7fd3d377ad2d9e7983d457b71ff8a04c0f19e41948d722f15c508a48dcce3
COPY commerce.sql grant.sh dataset.csv /docker-entrypoint-initdb.d/
COPY mysql84.cnf /etc/mysql/conf.d/mysql84.cnf
