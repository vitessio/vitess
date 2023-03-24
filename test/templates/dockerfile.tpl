ARG bootstrap_version=16
ARG image="vitess/bootstrap:${bootstrap_version}-{{.Platform}}"

FROM "${image}"

USER root

# Re-copy sources from working tree
RUN rm -rf /vt/src/vitess.io/vitess/*
COPY . /vt/src/vitess.io/vitess

{{if .InstallXtraBackup}}
# install XtraBackup
RUN wget https://repo.percona.com/apt/percona-release_latest.$(lsb_release -sc)_all.deb
RUN apt-get update
RUN apt-get install -y gnupg2
RUN dpkg -i percona-release_latest.$(lsb_release -sc)_all.deb
RUN apt-get update
RUN apt-get install -y percona-xtrabackup-24
{{end}}

# Set the working directory
WORKDIR /vt/src/vitess.io/vitess

# Fix permissions
RUN chown -R vitess:vitess /vt

USER vitess

# Set environment variables
ENV VTROOT /vt/src/vitess.io/vitess
# Set the vtdataroot such that it uses the volume mount
ENV VTDATAROOT /vt/vtdataroot

# create the vtdataroot directory
RUN mkdir -p $VTDATAROOT

# install goimports
RUN go install golang.org/x/tools/cmd/goimports@latest

{{if .MakeTools}}
# make tools
RUN make tools
{{end}}

# sleep for 50 minutes
CMD sleep 3000
