# docker-registry

As developers, we'd like to be able to use the convenience and intentions implied by using simple tags, but also the reproducibility of explicit dependencies.

To that end, docker-registry represents a mapping from image names to image IDs.

Projects that depend on external images can get the appropriate imageId:

```groovy
def imageId = Docker.lookupImageId(project, 'nginx:latest')
```

To verify the image IDs match between your system and docker-registry, run:

```shell
./gradlew docker-registry:verifyImages --continue
```

To pull and then verify, run:

```shell
./gradlew docker-registry:pullImages docker-registry:verifyImages --continue
```

To bring the images up-to-date, you can run:

```shell
./gradlew docker-registry:createPatch
rm -r docker/registry/images
mv docker/registry/build/images.patch docker/registry/images
```
