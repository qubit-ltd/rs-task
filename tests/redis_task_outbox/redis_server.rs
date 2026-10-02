// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Docker-backed disposable Redis server for integration tests.

use std::error::Error;
use std::net::SocketAddr;
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;

use redis::Client;

/// Owns a Redis container and removes it when dropped.
pub struct RedisServer {
    container_id: String,
    url: String,
}

impl RedisServer {
    /// Starts an owned Redis 7 container with an ephemeral local host port.
    ///
    /// Returns the ready fixture. Blocks on Docker/process and readiness IO;
    /// returns Docker, port-discovery, output-decoding, or readiness failure
    /// errors.
    pub fn start() -> Result<Self, Box<dyn Error>> {
        Self::start_version("7-alpine")
    }

    /// Starts a container for Redis image tag `image_tag` on an ephemeral local
    /// port.
    ///
    /// Returns the ready owned fixture. Performs Docker/process and readiness
    /// IO; returns Docker, port-discovery, output-decoding, or readiness failure
    /// errors. The partially created fixture cleans up its container if
    /// readiness fails.
    pub fn start_version(image_tag: &str) -> Result<Self, Box<dyn Error>> {
        let image = format!("redis:{image_tag}");
        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "-p",
                "127.0.0.1::6379",
                &image,
                "redis-server",
                "--appendonly",
                "yes",
            ])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "docker run failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        let container_id = String::from_utf8(output.stdout)?.trim().to_owned();
        let mut server = Self {
            container_id,
            url: String::new(),
        };
        server.refresh_url()?;
        for _ in 0..50 {
            if Client::open(server.url.as_str())
                .and_then(|client| client.get_connection())
                .is_ok()
            {
                return Ok(server);
            }
            sleep(Duration::from_millis(100));
        }
        Err("Redis container did not become ready".into())
    }

    /// Resolves Docker's assigned loopback port after container start or restart.
    /// Returns process, mapping, or address-parse errors; the owned container remains
    /// guarded by Drop if discovery fails.
    fn refresh_url(&mut self) -> Result<(), Box<dyn Error>> {
        let output = Command::new("docker")
            .args(["port", &self.container_id, "6379/tcp"])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "could not resolve Redis port: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        let mapping = String::from_utf8(output.stdout)?;
        let address: SocketAddr = mapping.trim().parse()?;
        if !address.ip().is_loopback() {
            return Err("isolated Redis port must bind to loopback".into());
        }
        self.url = format!("redis://{address}/");
        Ok(())
    }

    /// Stops the owned container and returns only after Docker confirms it stopped.
    /// This supplies an observed outage boundary without relying on elapsed time.
    pub fn stop(&self) -> Result<(), Box<dyn Error>> {
        let output = Command::new("docker")
            .args(["stop", &self.container_id])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "could not stop isolated Redis: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(())
    }

    /// Restarts this fixture's owned Redis container and waits for readiness.
    ///
    /// Returns success when connections work again. Performs blocking
    /// Docker/Redis IO and returns process, unsuccessful restart, or
    /// readiness failure errors.
    pub fn restart(&mut self) -> Result<(), Box<dyn Error>> {
        let output = Command::new("docker")
            .args(["restart", &self.container_id])
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "could not restart the isolated Redis server: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        self.refresh_url()?;
        for _ in 0..100 {
            if Client::open(self.url.as_str())
                .and_then(|client| client.get_connection())
                .is_ok()
            {
                return Ok(());
            }
            sleep(Duration::from_millis(100));
        }
        let logs = Command::new("docker")
            .args(["logs", "--tail", "30", &self.container_id])
            .output()?;
        Err(format!(
            "Redis container did not become ready after restart: {}",
            String::from_utf8_lossy(&logs.stdout)
        )
        .into())
    }

    /// Returns the connection URL borrowed from this fixture without
    /// allocating.
    #[must_use]
    #[inline]
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for RedisServer {
    /// Removes the container owned by this test fixture.
    ///
    /// Blocks on Docker process IO; cleanup errors are deliberately ignored.
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container_id])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}
