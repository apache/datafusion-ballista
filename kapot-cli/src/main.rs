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

use std::env;
use std::path::Path;

use kapot::prelude::{KapotConfig, KapotContext, Result};
use kapot_cli::{
    exec, print_format::PrintFormat, print_options::PrintOptions, KAPOT_CLI_VERSION,
};
use clap::Parser;
use datafusion_cli::print_options::MaxRows;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(
        short = 'p',
        long,
        help = "Path to your data, default to current directory",
        value_parser(parse_valid_data_dir)
    )]
    data_path: Option<String>,

    #[clap(
        short = 'c',
        long,
        help = "The batch size of each query, or use kapot default",
        value_parser(parse_batch_size)
    )]
    batch_size: Option<usize>,

    #[clap(
        long,
        help = "The max concurrent tasks, only for kapot local mode. Default: all available cores",
        value_parser(parse_valid_concurrent_tasks_size)
    )]
    concurrent_tasks: Option<usize>,

    #[clap(
        short,
        long,
        num_args = 1..,
        help = "Execute commands from file(s), then exit",
        value_parser(parse_valid_file)
    )]
    file: Vec<String>,

    #[clap(
        short = 'r',
        long,
        num_args = 1..,
        help = "Run the provided files on startup instead of ~/.kapotrc",
        value_parser(parse_valid_file),
        conflicts_with = "file"
    )]
    rc: Option<Vec<String>>,

    #[clap(long, value_enum, default_value_t = PrintFormat::Table)]
    format: PrintFormat,

    #[clap(long, help = "kapot scheduler host")]
    host: Option<String>,

    #[clap(long, help = "kapot scheduler port")]
    port: Option<u16>,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,

    #[clap(long, help = "Enables console syntax highlighting")]
    color: bool,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    if !args.quiet {
        println!("kapot CLI v{KAPOT_CLI_VERSION}");
    }

    if let Some(ref path) = args.data_path {
        let p = Path::new(path);
        env::set_current_dir(p).unwrap();
    };

    let mut kapot_config_builder =
        KapotConfig::builder().set("kapot.with_information_schema", "true");

    if let Some(batch_size) = args.batch_size {
        kapot_config_builder =
            kapot_config_builder.set("kapot.batch.size", &batch_size.to_string());
    };

    let kapot_config = kapot_config_builder.build()?;

    let ctx = match (args.host, args.port) {
        (Some(ref host), Some(port)) => {
            // Distributed execution with kapot Remote
            KapotContext::remote(host, port, &kapot_config).await?
        }
        _ => {
            let concurrent_tasks = if let Some(concurrent_tasks) = args.concurrent_tasks {
                concurrent_tasks
            } else {
                num_cpus::get()
            };
            // In-process execution with kapot Standalone
            KapotContext::standalone(&kapot_config, concurrent_tasks).await?
        }
    };

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
        maxrows: MaxRows::Unlimited,
        color: args.color,
    };

    let files = args.file;
    let rc = match args.rc {
        Option::Some(file) => file,
        Option::None => {
            let mut files = Vec::new();
            let home = dirs::home_dir();
            if let Some(p) = home {
                let home_rc = p.join(".kapotrc");
                if home_rc.exists() {
                    files.push(home_rc.into_os_string().into_string().unwrap());
                }
            }
            files
        }
    };
    if !files.is_empty() {
        exec::exec_from_files(files, &ctx, &print_options).await
    } else {
        if !rc.is_empty() {
            exec::exec_from_files(rc, &ctx, &print_options).await
        }
        exec::exec_from_repl(&ctx, &mut print_options).await;
    }

    Ok(())
}

fn parse_valid_file(dir: &str) -> std::result::Result<String, String> {
    if Path::new(dir).is_file() {
        Ok(dir.to_string())
    } else {
        Err(format!("Invalid file '{dir}'"))
    }
}

fn parse_valid_data_dir(dir: &str) -> std::result::Result<String, String> {
    if Path::new(dir).is_dir() {
        Ok(dir.to_string())
    } else {
        Err(format!("Invalid data directory '{dir}'"))
    }
}

fn parse_batch_size(size: &str) -> std::result::Result<usize, String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(size),
        _ => Err(format!("Invalid batch size '{size}'")),
    }
}

fn parse_valid_concurrent_tasks_size(size: &str) -> std::result::Result<usize, String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(size),
        _ => Err(format!("Invalid concurrent_tasks size '{size}'")),
    }
}
