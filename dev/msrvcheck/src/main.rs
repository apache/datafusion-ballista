// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use cargo::CargoResult;
use std::env;
use std::path::Path;

use cargo::util::context::GlobalContext;

/// Verifies that we are tracking the right MSRV from datafusion.
/// This is vastly inspired from <https://github.com/apache/datafusion/tree/10a437b826568c27b81d7d16a02b938a13d1a4ad/dev/depcheck>
fn main() -> CargoResult<()> {
    let gctx = GlobalContext::default()?;
    // This is the path for the depcheck binary
    let path = env::var("CARGO_MANIFEST_DIR").unwrap();
    let root_cargo_toml = Path::new(&path)
        // dev directory
        .parent()
        .expect("Can not find dev directory")
        // project root directory
        .parent()
        .expect("Can not find project root directory")
        .join("Cargo.toml");

    println!(
        "Checking for MSRV dependencies in {}",
        root_cargo_toml.display()
    );

    let workspace = cargo::core::Workspace::new(&root_cargo_toml, &gctx)?;
    let project_msrv = workspace.lowest_rust_version().unwrap(); // there should be a MSRV project wise

    let (_, resolve) = cargo::ops::resolve_ws(&workspace, false)?;
    let packages_with_rust_version: Vec<_> = resolve
        .iter()
        .filter(|id| id.name().starts_with("datafusion"))
        .map(|e| resolve.summary(e))
        .map(|e| (e.name(), e.rust_version()))
        .collect();

    println!("Current project MSRV: {}", project_msrv);

    for (package, version) in packages_with_rust_version {
        if let Some(v) = version {
            if !project_msrv.is_compatible_with(v.as_partial()) {
                panic!(
                    "package '{package}' has MSRV {v} not compatible with current project MSRV {project_msrv}",
                );
            }

            println!("{package} MSRV: {v}");
        }
    }

    println!("No inconsistent MSRV found");
    Ok(())
}
