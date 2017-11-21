[![master](https://img.shields.io/travis/emc-mongoose/mongoose-storage-driver-swift/master.svg)](https://travis-ci.org/emc-mongoose/mongoose-storage-driver-swift)
[![downloads](https://img.shields.io/github/downloads/emc-mongoose/mongoose-storage-driver-swift/total.svg)](https://github.com/emc-mongoose/mongoose-storage-driver-swift/releases)
[![release](https://img.shields.io/github/release/emc-mongoose/mongoose-storage-driver-swift.svg)]()
[![Docker Pulls](https://img.shields.io/docker/pulls/emcmongoose/mongoose-storage-driver-swift.svg)](https://hub.docker.com/r/emcmongoose/mongoose-storage-driver-swift/)

[Mongoose](https://github.com/emc-mongoose/mongoose-base)'s driver for
OpenStack Swift cloud storage

# Introduction

The storage driver extends the Mongoose's [Abstract HTTP Storage Driver](https://github.com/emc-mongoose/mongoose-base/wiki/v3.6-Extensions#231-http-storage-driver)

# Features

* API version: 1.0
* Authentification:
    * Uid/secret key pair for auth token operations
    * Auth token
* SSL/TLS
* Item types:
    * `data`
    * `path`
    * `token`
* Automatic auth token creation on demand
* Automatic destination path creation on demand
* Path listing input (with JSON response payload)
* Data item operation types:
    * `create`
        * copy
        * Dynamic Large Objects
    * `read`
        * full
        * random byte ranges
        * fixed byte ranges
        * content verification
    * `update`
        * full (overwrite)
        * random byte ranges
        * fixed byte ranges (with append mode)
    * `delete`
    * `noop`
* Token item operation types:
    * `create`
    * `noop`
* Path item operation types:
    * `create`
    * `read`
    * `delete`
    * `noop`

# Usage

Latest stable pre-built jar file is available at:
https://github.com/emc-mongoose/mongoose-storage-driver-swift/releases/download/latest/mongoose-storage-driver-swift.jar
This jar file may be downloaded manually and placed into the `ext`
directory of Mongoose to be automatically loaded into the runtime.

```bash
java -jar mongoose-<VERSION>/mongoose.jar \
    --storage-driver-type=swift \
    ...
```

## Notes

* To specify an auth token use the `storage-auth-token` configuration option
* Container may be specified with `item-input-path` either `item-output-path` configuration option
* DLO segments upload should be enabled using the `item-data-ranges-threshold` configuration option

## Docker

### Standalone

```bash
docker run \
    --network host \
    --entrypoint mongoose \
    emcmongoose/mongoose-storage-driver-swift \
    -jar /opt/mongoose/mongoose.jar \
    --storage-type=swift \
    ...
```

### Distributed

#### Drivers

```bash
docker run \
    --network host \
    --expose 1099 \
    emcmongoose/mongoose-storage-driver-service-swift
```

#### Controller

```bash
docker run \
    --network host \
    --entrypoint mongoose \
    emcmongoose/mongoose-base \
    -jar /opt/mongoose/mongoose.jar \
    --storage-driver-remote \
    --storage-driver-addrs=<ADDR1,ADDR2,...> \
    --storage-driver-type=swift \
    ...
```

## Advanced

### Sources

```bash
git clone https://github.com/emc-mongoose/mongoose-storage-driver-swift.git
cd mongoose-storage-driver-swift
```

### Test

```
./gradlew clean test
```

### Build

```bash

./gradlew clean jar
```

### Embedding

```groovy
compile group: 'com.github.emc-mongoose', name: 'mongoose-storage-driver-swift', version: '<VERSION>'
```

