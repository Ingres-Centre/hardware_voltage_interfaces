use nix::errno::Errno;
use nix::ioctl_read_buf;
use std::fs::{read_dir, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

ioctl_read_buf!(eviocgname, b'E', 0x06, u8);

pub fn enumerate_devices() -> std::io::Result<Vec<(PathBuf, String)>> {
    let mut results = Vec::new();
    let dir = read_dir("/dev/input")?;
    for entry in dir {
        let entry = entry?;
        let path = entry.path();
        if !path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.starts_with("event"))
            .unwrap_or(false)
        {
            continue;
        }

        let name = read_device_name(&path).unwrap_or_else(|_| "<unknown>".to_string());
        results.push((path, name));
    }
    Ok(results)
}

fn read_device_name(event_path: &Path) -> std::io::Result<String> {
    let file = OpenOptions::new().read(true).open(event_path)?;
    let mut buf = [0u8; 256];
    unsafe {
        if let Err(err) = eviocgname(file.as_raw_fd(), &mut buf) {
            let errno = (if err == Errno::UnknownErrno {
                Errno::EINVAL
            } else {
                err
            }) as i32;
            return Err(std::io::Error::from_raw_os_error(errno));
        }
    }
    let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
    Ok(String::from_utf8_lossy(&buf[..end]).trim().to_string())
}
