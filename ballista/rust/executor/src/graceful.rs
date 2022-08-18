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

use crate::shutdown::Shutdown;
use tokio::sync::{broadcast, mpsc};

pub struct Graceful {
    /// Broadcasts a shutdown signal to all related components.
    pub notify_shutdown: broadcast::Sender<()>,

    /// Used as part of the graceful shutdown process to wait for
    /// related components to complete processing.
    ///
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is
    /// leveraged to detect all shutdown processing completing.
    pub shutdown_complete_rx: mpsc::Receiver<()>,

    pub shutdown_complete_tx: mpsc::Sender<()>,
}

impl Graceful {
    /// Create a new Graceful instance
    pub fn new() -> Self {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        Self {
            notify_shutdown,
            shutdown_complete_rx,
            shutdown_complete_tx,
        }
    }

    /// Subscribe for shutdown notification
    pub fn subscribe_for_shutdown(&self) -> Shutdown {
        Shutdown::new(self.notify_shutdown.subscribe())
    }
}


impl Default for  Graceful {
    fn default() -> Self {
       Graceful::new()
    }
}
