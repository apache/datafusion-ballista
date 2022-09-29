<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Ballista Documentation

## User Documentation

Documentation for the current published release can be found at https://arrow.apache.org/ballista and the source
content is located [here](source/user-guide/introduction.md).

## Developer Documentation

Developer documentation can be found [here](developer/README.md).

## Building the User Guide

### Dependencies

It's recommended to install build dependencies and build the documentation
inside a Python virtualenv.

- Python
- `pip install -r requirements.txt`
- DataFusion python package. You can install the latest version by running `maturin develop` inside `../python` directory.

## Build

```bash
./build.sh
```

## Release

The documentation is served through the [arrow-site](https://github.com/apache/arrow-site/) repository. To release
a new version of the documentation, follow these steps:

1. Download the release source tarball (we can only publish documentation from official releases)
2. Run `./build.sh` inside `docs` folder to generate the docs website inside the `build/html` folder.
3. Clone the arrow-site repo
4. Checkout to the `asf-site` branch (NOT `master`)
5. Copy build artifacts into `arrow-site` repo's `ballista` folder with a command such as

- `cp -rT ./build/html/ ../../arrow-site/ballista/` (doesn't work on mac)
- `rsync -avzr ./build/html/ ../../arrow-site/ballista/`

6. Commit changes in `arrow-site` and send a PR.
