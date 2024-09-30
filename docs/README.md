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

# kapot Documentation

## User Documentation

Documentation for the current published release can be found at https://datafusion.apache.org/kapot and the source
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

The documentation is published from the `asf-site` branch of this repository.

Documentation is published automatically when documentation changes are pushed to the main branch.
