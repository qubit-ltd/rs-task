// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! TCP forwarding proxy and its lifecycle.

use std::io::BufReader;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;

use redis::Client;
use redis::ConnectionAddr;

use super::gate::ReplyGate;
#[path = "proxy/internal.rs"]
mod internal;

/// Pending raw response substitution: Some holds the uppercase command name
/// and replacement RESP bytes; None preserves the upstream response.
type ReplyReplacement = Option<(Vec<u8>, Vec<u8>)>;

/// Forwards RESP traffic and pauses after the selected command is applied.
pub struct ControlledRedis {
    address: String,
    stop: Arc<AtomicBool>,
    gate: Arc<ReplyGate>,
    replacement: Arc<Mutex<ReplyReplacement>>,
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    listener_thread: Option<JoinHandle<()>>,
}

impl ControlledRedis {
    /// Starts a TCP proxy in front of the Redis URL `upstream`.
    ///
    /// Returns the owned listener and forwarding workers; socket setup performs
    /// IO. Returns an IO error for an invalid URL, unsupported address,
    /// bind failure, or nonblocking-listener configuration failure. Thread
    /// creation may panic.
    pub fn start(upstream: &str) -> IoResult<Self> {
        let connection_info = Client::open(upstream)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "invalid Redis URL"))?
            .get_connection_info()
            .clone();
        let ConnectionAddr::Tcp(host, port) = connection_info.addr else {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "controlled Redis proxy supports TCP Redis endpoints",
            ));
        };
        let upstream = format!("{host}:{port}");
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?.to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(ReplyGate::default());
        let replacement = Arc::new(Mutex::new(None));
        let thread_replacement = Arc::clone(&replacement);
        let thread_stop = Arc::clone(&stop);
        let thread_gate = Arc::clone(&gate);
        let workers = Arc::new(Mutex::new(Vec::new()));
        let listener_workers = Arc::clone(&workers);
        let listener_thread = spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((client, _)) => {
                        let upstream = upstream.clone();
                        let gate = Arc::clone(&thread_gate);
                        let replacement = Arc::clone(&thread_replacement);
                        let worker = spawn(move || forward(client, &upstream, &gate, &replacement));
                        if let Ok(mut workers) = listener_workers.lock() {
                            workers.push(worker);
                        }
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            address,
            stop,
            gate,
            replacement,
            workers,
            listener_thread: Some(listener_thread),
        })
    }

    /// Returns a newly allocated Redis URL for this proxy's local TCP listener.
    pub fn url(&self) -> String {
        format!("redis://{}/", self.address)
    }

    /// Returns a shared handle to the reply gate controlled by this fixture.
    pub fn gate(&self) -> Arc<ReplyGate> {
        Arc::clone(&self.gate)
    }

    /// Copies raw RESP `reply` to replace the next applied reply for `command`.
    ///
    /// The command name is normalized to uppercase. State locking may block
    /// briefly and panics if the replacement mutex is poisoned; the proxy
    /// performs the IO.
    pub fn replace_next_reply(&self, command: &str, reply: &[u8]) {
        *self.replacement.lock().expect("reply replacement lock is healthy") =
            Some((command.as_bytes().to_ascii_uppercase(), reply.to_vec()));
    }

    /// Arms the next applied reply for `command` and returns a shared gate
    /// handle.
    ///
    /// The static command name is copied; no network IO is performed here.
    /// State locking may block briefly and panics if the gate mutex is
    /// poisoned.
    pub fn pause_after_reply(&self, command: &'static str) -> Arc<ReplyGate> {
        self.gate.arm_for(command);
        Arc::clone(&self.gate)
    }
}

impl Drop for ControlledRedis {
    /// Unblocks the gate, stops the listener, and joins owned forwarding
    /// threads.
    ///
    /// Performs socket IO and blocks until the threads finish; a poisoned gate
    /// mutex may panic while releasing the gate. Join errors are
    /// deliberately ignored.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.gate.release();
        let _ = TcpStream::connect(&self.address);
        if let Some(listener_thread) = self.listener_thread.take() {
            let _ = listener_thread.join();
        }
        if let Ok(mut workers) = self.workers.lock() {
            for worker in workers.drain(..) {
                let _ = worker.join();
            }
        }
    }
}

/// Relays the owned `client` connection to the TCP address `upstream`.
///
/// `gate` controls applied replies and `replacement` supplies one raw response.
/// Performs blocking socket IO and exits without a result on transport failure
/// or a discarded reply. Panics if the replacement mutex is poisoned.
fn forward(client: TcpStream, upstream: &str, gate: &ReplyGate, replacement: &Mutex<ReplyReplacement>) {
    let Ok(server) = TcpStream::connect(upstream) else {
        return;
    };
    let _ = client.set_read_timeout(Some(Duration::from_secs(3)));
    let _ = server.set_read_timeout(Some(Duration::from_secs(3)));
    let Ok(client_writer) = client.try_clone() else {
        return;
    };
    let Ok(server_writer) = server.try_clone() else {
        return;
    };
    let mut client_reader = BufReader::new(client);
    let mut server_reader = BufReader::new(server);
    let mut client_writer = client_writer;
    let mut server_writer = server_writer;
    while let Ok(Some((command, request))) = internal::read_request(&mut client_reader) {
        if server_writer.write_all(&request).is_err() || server_writer.flush().is_err() {
            break;
        }
        let Ok(mut response) = internal::read_response(&mut server_reader) else {
            break;
        };
        if gate.hold_if_armed(&command.to_ascii_uppercase(), &request) {
            break;
        }
        {
            let mut replacement = replacement.lock().expect("reply replacement lock is healthy");
            if replacement
                .as_ref()
                .is_some_and(|(name, _)| name.eq_ignore_ascii_case(&command))
            {
                let (_, bytes) = replacement.take().expect("matching replacement exists");
                response = bytes;
            }
        }
        if client_writer.write_all(&response).is_err() || client_writer.flush().is_err() {
            break;
        }
    }
}
