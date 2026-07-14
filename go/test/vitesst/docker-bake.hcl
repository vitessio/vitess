# Build the vitesst e2e test images. The usual entry point is:
#
#   make vitesst-images
#
# which cross-compiles the Vitess binaries into .vitesst_install/bin first.
# With the binaries in place the images can be baked directly:
#
#   docker buildx bake --load -f go/test/vitesst/docker-bake.hcl
#
# CI configures gha caching with e.g.
#   --set *.cache-from=type=gha,scope=vitesst-mysql84
#   --set *.cache-to=type=gha,mode=max,scope=vitesst-mysql84

group "default" {
  targets = ["mysql80", "mysql84"]
}

# MYSQL_VERSION pins must exist on cdn.mysql.com (the arm64 tarball path);
# Oracle rotates old patch releases off the CDN.
target "mysql80" {
  context    = "."
  dockerfile = "go/test/vitesst/Dockerfile"
  args = {
    FLAVOR        = "mysql80"
    BASE_IMAGE    = "debian:bookworm-slim"
    MYSQL_VERSION = "8.0.44"
  }
  tags = ["vitesst:mysql80"]
}

target "mysql84" {
  context    = "."
  dockerfile = "go/test/vitesst/Dockerfile"
  args = {
    FLAVOR        = "mysql84"
    BASE_IMAGE    = "debian:trixie-slim"
    MYSQL_VERSION = "8.4.8"
  }
  tags = ["vitesst:mysql84"]
}
