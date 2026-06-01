## vttestserver docker image

### How to build manually during a release

If for whatever reason the automatic build did not happen for `vttestserver` during a release, or if it has failed,
there is a way of building it by hand. Here is how it goes:

```bash
docker login

# we first checkout to the git tag of the release we want to build
# we assume in this example that we are releasing v21.0.0-rc1, but replace this by any other tag
git checkout v21.0.0-rc1
```

#### Single-platform builds

Build for a single architecture and load the image into your local Docker:

```bash
# x86 (amd64)
docker buildx build --platform=linux/amd64 -f docker/vttestserver/Dockerfile.mysql84 -t vitess/vttestserver:v21.0.0-rc1-mysql84 --load .

# ARM (arm64)
docker buildx build --platform=linux/arm64 -f docker/vttestserver/Dockerfile.mysql84 -t vitess/vttestserver:v21.0.0-rc1-mysql84 --load .
```
