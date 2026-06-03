// docker-bake.hcl defines the build targets for the Vitess bootstrap images.
//
// The bootstrap images provide a pre-configured environment for running
// Vitess tests in Docker. The common image contains shared dependencies,
// while flavor-specific images (e.g., mysql80) add database-specific packages.
//
// Usage:
//   docker buildx bake -f docker/bootstrap/docker-bake.hcl
//
// Variables can be overridden:
//   docker buildx bake -f docker/bootstrap/docker-bake.hcl --set *.tags=myregistry/bootstrap:mytag

variable "BOOTSTRAP_VERSION" {
  default = "ci"
}

variable "BOOTSTRAP_FLAVOR" {
  default = "mysql84"
}

variable "VITESS_ADDONS_DEPLOY_KEY" {
  default = ""
}

group "default" {
  targets = ["common", "flavor"]
}

target "common" {
  context    = "."
  dockerfile = "docker/bootstrap/Dockerfile.common"
  tags       = ["vitess/bootstrap:${BOOTSTRAP_VERSION}-common"]
  args = {
    VITESS_ADDONS_DEPLOY_KEY = VITESS_ADDONS_DEPLOY_KEY
  }
}

target "flavor" {
  context    = "."
  dockerfile = "docker/bootstrap/Dockerfile.${BOOTSTRAP_FLAVOR}"
  tags       = ["vitess/bootstrap:${BOOTSTRAP_VERSION}-${BOOTSTRAP_FLAVOR}"]

  contexts = {
    "vitess/bootstrap:${BOOTSTRAP_VERSION}-common" = "target:common"
  }

  args = {
    bootstrap_version = BOOTSTRAP_VERSION
    image             = "vitess/bootstrap:${BOOTSTRAP_VERSION}-common"
  }
}
