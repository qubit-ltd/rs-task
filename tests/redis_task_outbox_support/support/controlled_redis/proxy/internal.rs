// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RESP2 request and response framing helpers for the local proxy.

use std::io::BufReader;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Result as IoResult;
use std::net::TcpStream;
use std::str::from_utf8;

/// Reads one RESP2 request from `reader` without changing forwarded wire bytes.
///
/// Returns Some(command-name bytes, complete wire bytes), or None for clean
/// EOF. Consumes blocking socket input. Returns an IO error for truncated
/// frames, invalid array/bulk headers, invalid bulk lengths, or socket read
/// failures.
pub(super) fn read_request(
    reader: &mut BufReader<TcpStream>,
) -> IoResult<Option<(Vec<u8>, Vec<u8>)>> {
    let Some((line, mut wire)) = read_line(reader)? else {
        return Ok(None);
    };
    if line.first() != Some(&b'*') {
        return Err(Error::new(ErrorKind::InvalidData, "expected RESP array"));
    }
    let count = parse_length(&line[1..])?;
    let mut command = Vec::new();
    for index in 0..count {
        let (header, mut header_wire) = read_line(reader)?.ok_or_else(unexpected_eof)?;
        if header.first() != Some(&b'$') {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "expected RESP bulk string",
            ));
        }
        let length = usize::try_from(parse_length(&header[1..])?)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "invalid RESP bulk length"))?;
        let mut value = vec![0; length];
        reader.read_exact(&mut value)?;
        let mut ending = [0; 2];
        reader.read_exact(&mut ending)?;
        header_wire.extend_from_slice(&value);
        header_wire.extend_from_slice(&ending);
        wire.extend_from_slice(&header_wire);
        if index == 0 {
            command = value;
        }
    }
    Ok(Some((command, wire)))
}

/// Reads one complete RESP2 response from `reader`, including nested arrays.
///
/// Returns the exact response bytes. Consumes blocking fixture socket input and
/// returns an IO error for missing frames, invalid lengths, or failed reads.
pub(super) fn read_response(reader: &mut BufReader<TcpStream>) -> IoResult<Vec<u8>> {
    let (line, mut wire) = read_line(reader)?.ok_or_else(unexpected_eof)?;
    match line.first() {
        Some(b'$') => {
            let length = parse_length(&line[1..])?;
            if length >= 0 {
                let mut body = vec![0; length as usize + 2];
                reader.read_exact(&mut body)?;
                wire.extend_from_slice(&body);
            }
        }
        Some(b'*') => {
            let count = parse_length(&line[1..])?;
            if count >= 0 {
                for _ in 0..count {
                    wire.extend_from_slice(&read_response(reader)?);
                }
            }
        }
        _ => {}
    }
    Ok(wire)
}

/// Reads a CRLF-terminated line from `reader`, retaining its exact wire bytes.
///
/// Returns Some(line without CRLF, complete wire), or None for clean initial
/// EOF. Performs blocking reads; returns IO errors for a truncated line or read
/// failure.
fn read_line(reader: &mut BufReader<TcpStream>) -> IoResult<Option<(Vec<u8>, Vec<u8>)>> {
    let mut wire = Vec::new();
    let mut byte = [0];
    loop {
        match reader.read_exact(&mut byte) {
            Ok(()) => wire.push(byte[0]),
            Err(error) if error.kind() == ErrorKind::UnexpectedEof && wire.is_empty() => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
        if wire.ends_with(b"\r\n") {
            return Ok(Some((wire[..wire.len() - 2].to_vec(), wire)));
        }
    }
}

/// Parses the signed RESP length encoded in `bytes`.
///
/// Returns the signed length, including negative RESP sentinel lengths.
/// Returns InvalidData for invalid UTF-8 or text outside the isize integer
/// range.
fn parse_length(bytes: &[u8]) -> IoResult<isize> {
    from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "invalid RESP length"))
}

/// Returns an UnexpectedEof IO error describing a truncated RESP response.
fn unexpected_eof() -> Error {
    Error::new(ErrorKind::UnexpectedEof, "truncated RESP response")
}
