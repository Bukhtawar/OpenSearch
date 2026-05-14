/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Unix domain socket listener for heap profiling commands.
//!
//! Starts a background thread that listens on `<socket_path>` for simple text commands:
//!   - `activate` — enable jemalloc heap profiling
//!   - `deactivate` — disable jemalloc heap profiling
//!   - `dump <path>` — dump heap profile to the specified file
//!   - `reset <lg_sample>` — reset profiling state with new sample interval
//!   - `status` — report whether profiling is active
//!
//! This provides a jcmd-equivalent interface for native heap profiling that works
//! even when the node cannot join the cluster (no REST API dependency).

use std::ffi::CString;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;

use crate::error::ffm_wrap;

/// Starts the heap profiling UDS listener on a background thread.
/// Called from Java via FFI during NativeBridgeModule initialization.
///
/// The socket is created at `socket_path`. If the file already exists, it is removed first.
/// The listener thread runs for the lifetime of the process.
#[no_mangle]
pub extern "C" fn native_jemalloc_heap_prof_start_listener(path: *const std::ffi::c_char) -> i64 {
    ffm_wrap("native_jemalloc_heap_prof_start_listener", || {
        if path.is_null() {
            return Err("null socket path".to_string());
        }
        let c_str = unsafe { std::ffi::CStr::from_ptr(path) };
        let socket_path = c_str.to_str().map_err(|e| format!("invalid path: {}", e))?.to_owned();

        // Remove stale socket file if it exists
        let _ = std::fs::remove_file(&socket_path);

        let listener = UnixListener::bind(&socket_path)
            .map_err(|e| format!("failed to bind UDS at {}: {}", socket_path, e))?;

        // Set socket permissions to owner-only (600)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600));
        }

        log::info!("Heap profiling listener started at {}", socket_path);

        std::thread::Builder::new()
            .name("heap-prof-listener".to_string())
            .spawn(move || {
                for stream in listener.incoming() {
                    match stream {
                        Ok(stream) => handle_connection(stream),
                        Err(e) => log::warn!("Heap prof listener accept error: {}", e),
                    }
                }
            })
            .map_err(|e| format!("failed to spawn listener thread: {}", e))?;

        Ok(0i64)
    })
}

fn handle_connection(stream: std::os::unix::net::UnixStream) {
    let mut reader = BufReader::new(&stream);
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }

    let parts: Vec<&str> = line.trim().splitn(2, ' ').collect();
    let response = match parts[0] {
        "activate" => match unsafe { tikv_jemalloc_ctl::raw::write(b"prof.active\0", true) } {
            Ok(_) => "OK: profiling activated\n".to_string(),
            Err(e) => format!("ERR: failed to activate: {}\n", e),
        },
        "deactivate" => match unsafe { tikv_jemalloc_ctl::raw::write(b"prof.active\0", false) } {
            Ok(_) => "OK: profiling deactivated\n".to_string(),
            Err(e) => format!("ERR: failed to deactivate: {}\n", e),
        },
        "dump" => {
            let path = parts.get(1).unwrap_or(&"/tmp/heap.prof");
            match CString::new(*path) {
                Ok(c_path) => {
                    match unsafe {
                        tikv_jemalloc_ctl::raw::write(
                            b"prof.dump\0",
                            c_path.as_ptr() as *const std::ffi::c_char,
                        )
                    } {
                        Ok(_) => format!("OK: dumped to {}\n", path),
                        Err(e) => format!("ERR: failed to dump: {}\n", e),
                    }
                }
                Err(e) => format!("ERR: invalid path: {}\n", e),
            }
        }
        "reset" => {
            let lg: usize = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(17);
            match unsafe { tikv_jemalloc_ctl::raw::write(b"prof.reset\0", lg) } {
                Ok(_) => format!("OK: reset with lg_prof_sample={}\n", lg),
                Err(e) => format!("ERR: failed to reset: {}\n", e),
            }
        }
        "status" => {
            let result: Result<bool, _> = unsafe { tikv_jemalloc_ctl::raw::read(b"prof.active\0") };
            match result {
                Ok(active) => format!("OK: prof_active={}\n", active),
                Err(e) => format!("ERR: failed to read status: {}\n", e),
            }
        }
        _ => "ERR: unknown command. Use: activate|deactivate|dump <path>|reset <lg>|status\n"
            .to_string(),
    };

    let _ = (&stream).write_all(response.as_bytes());
}
