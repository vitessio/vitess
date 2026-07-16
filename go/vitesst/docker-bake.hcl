# Build the vitesst e2e test images. The usual entry point is:
#
#   make vitesst_images
#
# which cross-compiles the Vitess binaries into .vitesst_install/bin first.
# With the binaries in place the images can be baked directly:
#
#   docker buildx bake --load -f go/vitesst/docker-bake.hcl
#
# CI configures gha caching with e.g.
#   --set *.cache-from=type=gha,scope=vitesst-mysql84
#   --set *.cache-to=type=gha,mode=max,scope=vitesst-mysql84

group "default" {
  targets = ["mysql80", "mysql84", "mariadb"]
}

# MYSQL_VERSION pins must exist on cdn.mysql.com (the arm64 tarball path);
# Oracle rotates old patch releases off the CDN.
target "mysql80" {
  context    = "."
  dockerfile = "go/vitesst/Dockerfile"
  args = {
    FLAVOR        = "mysql80"
    BASE_IMAGE    = "debian:bookworm-slim"
    MYSQL_VERSION = "8.0.44"
  }
  tags = ["vitesst:mysql80"]
}

target "mysql84" {
  context    = "."
  dockerfile = "go/vitesst/Dockerfile"
  args = {
    FLAVOR        = "mysql84"
    BASE_IMAGE    = "debian:trixie-slim"
    MYSQL_VERSION = "8.4.8"
  }
  tags = ["vitesst:mysql84"]
}

# MariaDB comes from Debian bookworm's own repositories, which carry the 10.11
# long term release. Keyspaces select it with WithImage("vitesst:mariadb"), for
# workflows that move data from MariaDB to MySQL.
target "mariadb" {
  context    = "."
  dockerfile = "go/vitesst/Dockerfile"
  args = {
    FLAVOR     = "mariadb"
    BASE_IMAGE = "debian:bookworm-slim"
  }
  tags = ["vitesst:mariadb"]
}

# Built on demand for the transaction/twopc suites, whose fault injection
# needs binaries compiled with -tags debug2PC:
#   make vitesst-images-debug2pc
target "mysql84-debug2pc" {
  context    = "."
  dockerfile = "go/vitesst/Dockerfile"
  args = {
    FLAVOR        = "mysql84"
    BASE_IMAGE    = "debian:trixie-slim"
    MYSQL_VERSION = "8.4.8"
    BIN_DIR       = ".vitesst_install_debug2pc/bin"
  }
  tags = ["vitesst:mysql84-debug2pc"]
}
