// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Disposable Redis server for integration tests, using Docker when available
//! and an installed Redis process otherwise.

use std::error::Error;
use std::fs;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::thread::sleep;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use redis::Client;

enum RedisBackend {
    Docker { container_id: String },
    Process { child: Child, data_dir: PathBuf, port: u16 },
}

/// Owns a disposable Redis server and removes its resources when dropped.
pub struct RedisServer {
    backend: RedisBackend,
    url: String,
}

impl RedisServer {
    /// Starts an owned Redis 7 server with append-only persistence enabled.
    ///
    /// Docker is preferred. If Docker cannot start the container, an installed
    /// `redis-server` executable is used. Both options run a real Redis server.
    pub fn start() -> Result<Self, Box<dyn Error>> {
        Self::start_version("7-alpine")
    }

    /// Starts Redis using Docker image tag `image_tag` when Docker is
    /// available, falling back to the installed local Redis server.
    pub fn start_version(image_tag: &str) -> Result<Self, Box<dyn Error>> {
        match Self::start_docker(image_tag) {
            Ok(server) => Ok(server),
            Err(docker_error) => Self::start_local().map_err(|local_error| {
                format!("could not start Redis with Docker ({docker_error}) or local redis-server ({local_error})")
                    .into()
            }),
        }
    }

    fn start_docker(image_tag: &str) -> Result<Self, Box<dyn Error>> {
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
            return Err(format!("docker run failed: {}", String::from_utf8_lossy(&output.stderr)).into());
        }
        let container_id = String::from_utf8(output.stdout)?.trim().to_owned();
        let mut server = Self {
            backend: RedisBackend::Docker { container_id },
            url: String::new(),
        };
        server.refresh_url()?;
        server.wait_until_ready(50)?;
        Ok(server)
    }

    fn start_local() -> Result<Self, Box<dyn Error>> {
        let port = TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port();
        let data_dir = unique_data_directory()?;
        let child = match spawn_redis(port, &data_dir) {
            Ok(child) => child,
            Err(error) => {
                let _ = fs::remove_dir_all(&data_dir);
                return Err(error);
            }
        };
        let server = Self {
            backend: RedisBackend::Process { child, data_dir, port },
            url: format!("redis://127.0.0.1:{port}/"),
        };
        server.wait_until_ready(50)?;
        Ok(server)
    }

    /// Stops the owned Redis server and waits for its process/container to
    /// exit.
    pub fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        match &mut self.backend {
            RedisBackend::Docker { container_id } => {
                let output = Command::new("docker").args(["stop", container_id]).output()?;
                if !output.status.success() {
                    return Err(format!(
                        "could not stop isolated Redis: {}",
                        String::from_utf8_lossy(&output.stderr)
                    )
                    .into());
                }
            }
            RedisBackend::Process { child, port, .. } => {
                if let Ok(client) = Client::open(format!("redis://127.0.0.1:{port}/"))
                    && let Ok(mut connection) = client.get_connection()
                {
                    // Redis handles SHUTDOWN gracefully, flushing the AOF
                    // before exit. The response may be lost as the socket closes.
                    let _ = redis::cmd("SHUTDOWN").query::<()>(&mut connection);
                }
                for _ in 0..100 {
                    if child.try_wait()?.is_some() {
                        return Ok(());
                    }
                    sleep(Duration::from_millis(10));
                }
                child.kill()?;
                let _ = child.wait()?;
            }
        }
        Ok(())
    }

    /// Restarts the owned server and waits until it accepts connections.
    pub fn restart(&mut self) -> Result<(), Box<dyn Error>> {
        match &mut self.backend {
            RedisBackend::Docker { container_id } => {
                let output = Command::new("docker").args(["restart", container_id]).output()?;
                if !output.status.success() {
                    return Err(format!(
                        "could not restart the isolated Redis server: {}",
                        String::from_utf8_lossy(&output.stderr)
                    )
                    .into());
                }
                self.refresh_url()?;
            }
            RedisBackend::Process { child, data_dir, port } => {
                *child = spawn_redis(*port, data_dir)?;
            }
        }
        self.wait_until_ready(100)
    }

    /// Returns the Redis connection URL borrowed from this fixture.
    #[must_use]
    #[inline]
    pub fn url(&self) -> &str {
        &self.url
    }

    fn wait_until_ready(&self, attempts: usize) -> Result<(), Box<dyn Error>> {
        for _ in 0..attempts {
            if Client::open(self.url.as_str())
                .and_then(|client| client.get_connection())
                .is_ok()
            {
                return Ok(());
            }
            sleep(Duration::from_millis(100));
        }
        Err(format!("Redis server did not become ready at {}", self.url).into())
    }

    fn refresh_url(&mut self) -> Result<(), Box<dyn Error>> {
        let RedisBackend::Docker { container_id } = &self.backend else {
            return Ok(());
        };
        let output = Command::new("docker")
            .args(["port", container_id, "6379/tcp"])
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
}

fn unique_data_directory() -> Result<PathBuf, Box<dyn Error>> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let directory = std::env::temp_dir().join(format!("qubit-task-redis-{}-{timestamp}", std::process::id()));
    fs::create_dir(&directory)?;
    Ok(directory)
}

fn spawn_redis(port: u16, data_dir: &Path) -> Result<Child, Box<dyn Error>> {
    let output = data_dir.join("redis.log");
    let log = fs::File::create(output)?;
    let child = Command::new("redis-server")
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--dir",
            data_dir.to_str().ok_or("Redis data directory is not UTF-8")?,
            "--appendonly",
            "yes",
            "--save",
            "",
            "--protected-mode",
            "no",
            "--daemonize",
            "no",
        ])
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .spawn()?;
    Ok(child)
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        match &mut self.backend {
            RedisBackend::Docker { container_id } => {
                let _ = Command::new("docker")
                    .args(["rm", "-f", container_id])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
            RedisBackend::Process { child, data_dir, .. } => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = fs::remove_dir_all(data_dir);
            }
        }
    }
}
