#![feature(portable_simd)]

use crate::custom_hash::{FastHashBuilder, FastHashMap};
use libc::{c_int, memchr};
use std::{
    collections::BTreeMap,
    fs::File,
    os::{fd::AsRawFd, raw::c_void},
};
mod custom_hash;

struct StationData {
    total: i64,
    min: i64,
    max: i64,
    accumulate: i64,
}

impl Default for StationData {
    fn default() -> Self {
        Self {
            total: 0,
            min: i64::MAX,
            max: i64::MIN,
            accumulate: 0,
        }
    }
}

fn main() {
    let mut stations: FastHashMap<&[u8], StationData> =
        FastHashMap::with_capacity_and_hasher(413, FastHashBuilder::default());
    let file = File::open("measurements/measurements.txt").unwrap();

    let map = new(&file);
    let mut at = 0;
    loop {
        let line = parse_line(map, &mut at);
        if line.is_empty() {
            break;
        }

        let (station, temp) = split_deli(line);
        let temp = parse_temp(temp);
        match stations.get_mut(station) {
            Some(entry) => {
                entry.total += 1;

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
                    station,
                    StationData {
                        total: 1,
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
            .map(|(k, v)| (unsafe { std::str::from_utf8_unchecked(k) }, v)),
    );

    for (station, stats) in stations {
        print!(
            "{{{:?}={}/{:.1}/{:.1}}}, ",
            station,
            stats.min as f64 / 10.0,
            (stats.accumulate as f64 / stats.total as f64) / 10.0,
            stats.max as f64 / 10.0
        );
    }
}

fn parse_line<'a>(map: &'a [u8], pos: &mut usize) -> &'a [u8] {
    let current = &map[*pos..];
    let next_newline = unsafe {
        memchr(
            current.as_ptr() as *const c_void,
            b'\n' as c_int,
            current.len(),
        )
    };
    let line = if next_newline.is_null() {
        current
    } else {
        let index = next_newline as usize - current.as_ptr() as usize;
        &current[..index]
    };

    *pos += line.len() + 1;
    line
}

fn split_deli(line: &[u8]) -> (&[u8], &[u8]) {
    let index = unsafe { memchr(line.as_ptr() as *const c_void, b';' as c_int, line.len()) };

    let index = Some(index as usize - line.as_ptr() as usize).unwrap();

    unsafe { (line.get_unchecked(..index), line.get_unchecked(index + 1..)) }
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

fn parse_temp(bytes: &[u8]) -> i64 {
    let (neg, bytes) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };

    let val: i64 = match bytes.len() {
        3 => (bytes[0] - b'0') as i64 * 10 + (bytes[2] - b'0') as i64, // X.X
        4 => {
            (bytes[0] - b'0') as i64 * 100
                + (bytes[1] - b'0') as i64 * 10
                + (bytes[3] - b'0') as i64
        } // XX.X
        _ => unreachable!(),
    };

    if neg { -val } else { val }
}
