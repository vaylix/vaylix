//! Container-only bootstrap entrypoint for the Vaylix runtime image.
//!
//! Distroless images do not provide a shell, `chown`, `gosu`, or `su-exec`.
//! This binary replaces the old shell entrypoint: it prepares persistent
//! directories while running as root, drops to the configured runtime
//! UID/GID, and then `exec`s the actual server process.
//!
//! This is intentionally not a public CLI surface. It exists so Linux
//! bind-mounted data directories work out of the box while the long-running
//! database process still runs without root privileges.

#[cfg(not(unix))]
fn main() {
    eprintln!("[vaylix-init] this container init binary is only supported on Unix targets");
    std::process::exit(1);
}

#[cfg(unix)]
fn main() {
    unix_init::main();
}

#[cfg(unix)]
mod unix_init {
    use std::env;
    use std::ffi::CString;
    use std::fs;
    use std::io;
    use std::os::unix::ffi::OsStrExt;
    use std::path::{Path, PathBuf};

    const DEFAULT_DATA_DIR: &str = "/var/lib/vaylix";
    const DEFAULT_UID: u32 = 65_532;
    const DEFAULT_GID: u32 = 65_532;

    pub(super) fn main() {
        if let Err(err) = run() {
            eprintln!("[vaylix-init] {err}");
            std::process::exit(1);
        }
    }

    /// Prepare storage ownership, drop privileges, and replace this process with
    /// the command passed through Docker `CMD`.
    ///
    /// Environment inputs:
    ///
    /// - `VAYLIX_DATA_DIR`: persistent database root, default `/var/lib/vaylix`
    /// - `VAYLIX_BACKUP_DIR`: logical backup root, default `<data-dir>/backups`
    /// - `VAYLIX_RUNTIME_UID`: target server UID, default `65532`
    /// - `VAYLIX_RUNTIME_GID`: target server GID, default `65532`
    ///
    /// Failure is fail-fast by design. A partially bootstrapped data directory is
    /// safer than starting the server with ambiguous ownership or privileges.
    fn run() -> Result<(), String> {
        let command = env::args_os().skip(1).collect::<Vec<_>>();
        if command.is_empty() {
            return Err("no command supplied to exec".to_string());
        }

        let data_dir = env_path("VAYLIX_DATA_DIR", DEFAULT_DATA_DIR);
        let backup_dir = env_path(
            "VAYLIX_BACKUP_DIR",
            &format!("{}/backups", data_dir.display()),
        );
        let uid = env_u32("VAYLIX_RUNTIME_UID", DEFAULT_UID)?;
        let gid = env_u32("VAYLIX_RUNTIME_GID", DEFAULT_GID)?;

        fs::create_dir_all(&data_dir).map_err(|err| {
            format!(
                "failed to create data directory {}: {err}",
                data_dir.display()
            )
        })?;
        fs::create_dir_all(&backup_dir).map_err(|err| {
            format!(
                "failed to create backup directory {}: {err}",
                backup_dir.display()
            )
        })?;

        // Repair the data tree after bind mounts are attached. This is the reason
        // the init process must start as root instead of using distroless:nonroot.
        chown_recursive(&data_dir, uid, gid)
            .map_err(|err| format!("failed to chown {}: {err}", data_dir.display()))?;

        // The default backup directory is under the data root and is already
        // covered. A custom external backup path must be repaired separately.
        if !backup_dir.starts_with(&data_dir) {
            chown_recursive(&backup_dir, uid, gid)
                .map_err(|err| format!("failed to chown {}: {err}", backup_dir.display()))?;
        }

        set_identity(uid, gid)?;
        exec_command(&command)
    }

    /// Read a path environment variable without forcing Unicode conversion.
    fn env_path(name: &str, default: &str) -> PathBuf {
        env::var_os(name)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(default))
    }

    /// Read an unsigned UID/GID value from the environment.
    fn env_u32(name: &str, default: u32) -> Result<u32, String> {
        match env::var(name) {
            Ok(value) => value
                .parse::<u32>()
                .map_err(|err| format!("{name} must be an unsigned 32-bit integer: {err}")),
            Err(env::VarError::NotPresent) => Ok(default),
            Err(err) => Err(format!("failed to read {name}: {err}")),
        }
    }

    /// Recursively assign ownership to a directory tree.
    ///
    /// Symlinks are intentionally not followed: `symlink_metadata` plus `lchown`
    /// repairs the link object itself and avoids escaping the mounted data tree via
    /// a malicious symlink.
    fn chown_recursive(path: &Path, uid: u32, gid: u32) -> io::Result<()> {
        chown_path(path, uid, gid)?;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();
            let metadata = fs::symlink_metadata(&entry_path)?;
            chown_path(&entry_path, uid, gid)?;
            if metadata.is_dir() {
                chown_recursive(&entry_path, uid, gid)?;
            }
        }
        Ok(())
    }

    /// Assign ownership to one filesystem entry using `lchown`.
    fn chown_path(path: &Path, uid: u32, gid: u32) -> io::Result<()> {
        let c_path = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL byte"))?;

        // SAFETY: `c_path` is a valid NUL-terminated path pointer for the duration
        // of the call. UID/GID are plain integer values parsed from the environment.
        let result = unsafe { libc::lchown(c_path.as_ptr(), uid, gid) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Permanently drop group and user privileges for the current process.
    ///
    /// `setgid` is called before `setuid` so the process does not lose permission
    /// to switch groups first.
    fn set_identity(uid: u32, gid: u32) -> Result<(), String> {
        // SAFETY: `setgid` mutates only the current process credentials.
        let gid_result = unsafe { libc::setgid(gid) };
        if gid_result != 0 {
            return Err(format!(
                "failed to setgid({gid}): {}",
                io::Error::last_os_error()
            ));
        }

        // SAFETY: `setuid` mutates only the current process credentials.
        let uid_result = unsafe { libc::setuid(uid) };
        if uid_result != 0 {
            return Err(format!(
                "failed to setuid({uid}): {}",
                io::Error::last_os_error()
            ));
        }

        Ok(())
    }

    /// Replace the init process with the target server command.
    ///
    /// On success this function never returns. A returned value means `execvp`
    /// failed, usually because the command path is missing or not executable.
    fn exec_command(command: &[std::ffi::OsString]) -> Result<(), String> {
        let argv = command
            .iter()
            .map(|arg| {
                CString::new(arg.as_os_str().as_bytes())
                    .map_err(|_| format!("argument contains NUL byte: {}", arg.to_string_lossy()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut argv_ptrs = argv
            .iter()
            .map(|arg| arg.as_ptr())
            .collect::<Vec<*const libc::c_char>>();
        argv_ptrs.push(std::ptr::null());

        // SAFETY: `argv` owns all C strings while `argv_ptrs` is used, and the
        // pointer array is NUL-terminated as required by `execvp`.
        let result = unsafe { libc::execvp(argv[0].as_ptr(), argv_ptrs.as_ptr()) };
        debug_assert_eq!(result, -1);
        Err(format!(
            "failed to exec {}: {}",
            command[0].to_string_lossy(),
            io::Error::last_os_error()
        ))
    }
}
