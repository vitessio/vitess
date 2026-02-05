## vttestserver docker image

### How to build manually during a release

If for whatever reason the automatic build did not happen for `vttestserver` during a release, or if it has failed,
there is a way of building it by hand. Here is how it goes:

```bash
docker login

# we first checkout to the git tag of the release we want to build
# we assume in this example that we are releasing v21.0.0-rc1, but replace this by any other tag
git checkout v21.0.0-rc1

docker build -f docker/vtttestserver/Dockerfile.mysql80 -t vitess/vttestserver:v21.0.0-rc1-mysql80 .
docker push vitess/vttestserver:v21.0.0-rc1-mysql80
```