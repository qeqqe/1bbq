use core::f64;
use std::{collections::BTreeMap, fs::File, os::fd::AsRawFd};

use crate::custom_hash::FastHashMap;
mod custom_hash;

struct StationData {
    total: f64,
    min: f64,
    max: f64,
    accumulate: f64,
}

impl Default for StationData {
    fn default() -> Self {
        Self {
            total: 0.0,
            min: f64::MAX,
            max: f64::MIN,
            accumulate: 0.0,
        }
    }
}

fn main() {
    let mut stations: FastHashMap<Vec<u8>, StationData> = FastHashMap::default();

    let file = File::open("measurements/measurements.txt").unwrap();

    // let reader = BufReader::new(file);

    let map = new(&file);

    for line in map.split(|l| *l == b'\n') {
        if line.is_empty() {
            break;
        }
        let mut fields = line.splitn(2, |c| *c == b';');
        let station = fields.next().unwrap();
        let temp: f64 = parse_temp(fields.next().unwrap()) as f64;
        match stations.get_mut(station) {
            Some(entry) => {
                entry.total += 1.0;

                if entry.max < temp {
                    entry.max = temp;
                }

                if entry.min > temp {
                    entry.min = temp;
                }

                entry.accumulate += temp;
            }
            None => {
                stations.insert(
                    station.to_owned(),
                    StationData {
                        total: 1.0,
                        min: temp,
                        max: temp,
                        accumulate: temp,
                    },
                );
            }
        }
    }

    let stations = BTreeMap::from_iter(
        stations
            .into_iter()
            .map(|(k, v)| (unsafe { String::from_utf8_unchecked(k) }, v)),
    );

    for (station, stats) in stations {
        print!(
            "{{{:?}={}/{:.1}/{:.1}}}, ",
            station,
            stats.min,
            stats.accumulate / stats.total,
            stats.max
        );
    }
}

fn new(f: &File) -> &'_ [u8] {
    let len = f.metadata().unwrap().len();
    unsafe {
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            len as libc::size_t,
            libc::PROT_READ,
            libc::MAP_SHARED,
            f.as_raw_fd(),
            0,
        );

        if ptr == libc::MAP_FAILED {
            panic!("{:?}", std::io::Error::last_os_error());
        } else {
            if libc::madvise(ptr, len as libc::size_t, libc::MADV_SEQUENTIAL) != 0 {
                panic!("{:?}", std::io::Error::last_os_error())
            }
            std::slice::from_raw_parts(ptr as *const u8, len as usize)
        }
    }
}

fn parse_temp(bytes: &[u8]) -> i32 {
    let (neg, bytes) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };

    let val = match bytes.len() {
        3 => (bytes[0] - b'0') as i32 * 10 + (bytes[2] - b'0') as i32,
        4 => {
            (bytes[0] - b'0') as i32 * 100
                + (bytes[1] - b'0') as i32 * 10
                + (bytes[3] - b'0') as i32
        } // XX.X
        _ => unreachable!(),
    };

    if neg { -val } else { val }
}
