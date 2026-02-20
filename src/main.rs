#![feature(portable_simd)]
use crate::probe_map::FastMap;
use libc::{c_int, memchr};
use std::{
    collections::BTreeMap,
    fs::File,
    os::{fd::AsRawFd, raw::c_void},
};

mod probe_map;

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
    let mut stations = FastMap::new();

    let map = new();
    let mut at = 0;

    loop {
        let line = parse_line(map, &mut at);
        if line.is_empty() {
            break;
        }

        let (station, temp_bytes) = split_deli(line);
        let temp = parse_temp(temp_bytes);
        let entry = stations.get_mut_or_create(station);

        entry.total += 1;
        entry.accumulate += temp;
        if temp < entry.min {
            entry.min = temp;
        }
        if temp > entry.max {
            entry.max = temp;
        }
    }

    let mut sorted_stations = BTreeMap::new();
    for i in 0..16384 {
        if stations.keys[i] != 0 {
            let name = unsafe {
                let s = std::slice::from_raw_parts(stations.keys[i] as *const u8, stations.lens[i]);
                std::str::from_utf8_unchecked(s)
            };
            sorted_stations.insert(name, &stations.values[i]);
        }
    }

    print!("{{");
    for (i, (station, stats)) in sorted_stations.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!(
            "{}={:.1}/{:.1}/{:.1}",
            station,
            stats.min as f64 / 10.0,
            (stats.accumulate as f64 / stats.total as f64) / 10.0,
            stats.max as f64 / 10.0
        );
    }
    println!("}}");
}
#[inline(always)]
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
#[inline(always)]
fn split_deli(line: &[u8]) -> (&[u8], &[u8]) {
    let index = unsafe { memchr(line.as_ptr() as *const c_void, b';' as c_int, line.len()) };

    let index = index as usize - line.as_ptr() as usize;

    unsafe { (line.get_unchecked(..index), line.get_unchecked(index + 1..)) }
}

#[inline(always)]
fn new<'a>() -> &'a [u8] {
    let f = File::open("measurements/measurements.txt").unwrap();
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
            if libc::madvise(ptr, len as libc::size_t, libc::MADV_HUGEPAGE) != 0
                && libc::madvise(ptr, len as libc::size_t, libc::MADV_SEQUENTIAL) != 0
            {
                panic!("{:?}", std::io::Error::last_os_error())
            }
            std::slice::from_raw_parts(ptr as *const u8, len as usize)
        }
    }
}

#[inline(always)]
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
