#![feature(portable_simd)]
use crate::probe_map::FastMap;
use libc::{c_int, memchr};
use std::{
    collections::BTreeMap,
    fs::File,
    os::{fd::AsRawFd, raw::c_void},
    thread,
};

mod probe_map;

struct StationData {
    min: i16,
    max: i16,
    count: u32,
    sum: i32,
}

impl Default for StationData {
    fn default() -> Self {
        Self {
            min: i16::MAX,
            max: i16::MIN,
            count: 0,
            sum: 0,
        }
    }
}

fn main() {
    let data = new();
    let len = data.len();

    let num_threads = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let chunk_size = len / num_threads;

    let maps = thread::scope(|s| {
        let mut handles = Vec::with_capacity(num_threads);
        let mut start = 0;

        for i in 0..num_threads {
            let mut end = if i == num_threads - 1 {
                len
            } else {
                let mut e = start + chunk_size;

                while e < len && data[e] != b'\n' {
                    e += 1;
                }
                e + 1
            };

            if start >= len {
                break;
            }
            if end > len {
                end = len;
            }

            let slice = &data[start..end];

            handles.push(s.spawn(move || process_chunk(slice)));

            start = end;
        }

        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });

    let mut final_map = FastMap::new();

    for map in maps {
        for i in 0..probe_map::CAP {
            if map.keys[i] != 0 {
                let key_ptr = map.keys[i] as *const u8;
                let key_len = map.lens[i];
                let key_slice = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };

                let val = &map.values[i];

                let entry = final_map.get_mut_or_create(key_slice);

                if val.min < entry.min {
                    entry.min = val.min;
                }
                if val.max > entry.max {
                    entry.max = val.max;
                }
                entry.sum += val.sum;
                entry.count += val.count;
            }
        }
    }

    let mut sorted_stations = BTreeMap::new();
    for i in 0..probe_map::CAP {
        if final_map.keys[i] != 0 {
            let name = unsafe {
                let s =
                    std::slice::from_raw_parts(final_map.keys[i] as *const u8, final_map.lens[i]);
                std::str::from_utf8_unchecked(s)
            };
            sorted_stations.insert(name, &final_map.values[i]);
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
            (stats.sum as f64 / stats.count as f64) / 10.0,
            stats.max as f64 / 10.0
        );
    }
    println!("}}");
}

fn process_chunk(chunk: &[u8]) -> FastMap {
    let mut map = FastMap::new();
    let mut ptr = chunk.as_ptr();
    let end = unsafe { ptr.add(chunk.len()) };

    while ptr < end {
        unsafe {
            let mut semi_ptr = memchr(ptr as *const c_void, b';' as c_int, 100) as *const u8;

            if semi_ptr.is_null() {
                semi_ptr = memchr(
                    ptr as *const c_void,
                    b';' as c_int,
                    (end as usize) - (ptr as usize),
                ) as *const u8;
            }

            let name_len = semi_ptr as usize - ptr as usize;
            let name_slice = std::slice::from_raw_parts(ptr, name_len);

            let num_start = semi_ptr.add(1);

            let val: i16;
            let next_line: *const u8;

            let b0 = *num_start;
            if b0 == b'-' {
                let b1 = *num_start.add(1);
                if *num_start.add(3) == b'.' {
                    val = -((b1 as i16 - 48) * 100
                        + (*num_start.add(2) as i16 - 48) * 10
                        + (*num_start.add(4) as i16 - 48));
                    next_line = num_start.add(6);
                } else {
                    val = -((b1 as i16 - 48) * 10 + (*num_start.add(3) as i16 - 48));
                    next_line = num_start.add(5);
                }
            } else {
                if *num_start.add(2) == b'.' {
                    val = (b0 as i16 - 48) * 100
                        + (*num_start.add(1) as i16 - 48) * 10
                        + (*num_start.add(3) as i16 - 48);
                    next_line = num_start.add(5);
                } else {
                    val = (b0 as i16 - 48) * 10 + (*num_start.add(2) as i16 - 48);
                    next_line = num_start.add(4);
                }
            }

            let entry = map.get_mut_or_create(name_slice);
            entry.count += 1;
            entry.sum += val as i32;
            if val < entry.min {
                entry.min = val;
            }
            if val > entry.max {
                entry.max = val;
            }

            ptr = next_line;
        }
    }
    map
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
            panic!("mmap failed");
        }

        libc::madvise(ptr, len as libc::size_t, libc::MADV_HUGEPAGE);
        std::slice::from_raw_parts(ptr as *const u8, len as usize)
    }
}
