#![feature(portable_simd)]
use crate::probe_map::{CAP, FastMap, StationData};
use std::collections::BTreeMap;
use std::fs::File;
use std::os::fd::AsRawFd;
use std::thread;

mod probe_map;

fn main() {
    let data = new();
    let len = data.len();
    let ptr = data.as_ptr();

    let num_threads = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(16);
    let chunk_size = len / num_threads;

    let maps = thread::scope(|s| {
        let mut handles = Vec::with_capacity(num_threads);
        let mut start = 0;

        for i in 0..num_threads {
            let mut end = if i == num_threads - 1 {
                len
            } else {
                let mut e = start + chunk_size;
                unsafe {
                    while e < len && *ptr.add(e) != b'\n' {
                        e += 1;
                    }
                }
                e + 1
            };

            let chunk_addr = unsafe { ptr.add(start) } as usize;
            let chunk_len = end - start;

            handles.push(
                s.spawn(move || unsafe { process_chunk(chunk_addr as *const u8, chunk_len) }),
            );

            start = end;
        }

        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });

    let mut final_map = FastMap::new();
    for map in maps {
        for i in 0..CAP {
            if map.keys[i] != 0 {
                unsafe {
                    let key_ptr = map.keys[i] as *const u8;
                    let key_len = map.lens[i];

                    let first = (key_ptr as *const u64).read_unaligned();
                    let hash = if key_len >= 8 {
                        let last = (key_ptr.add(key_len - 8) as *const u64).read_unaligned();
                        first.rotate_left(5) ^ last
                    } else {
                        first
                    };

                    let entry = final_map.get_mut_or_create(key_ptr, key_len, hash);
                    let val = &map.values[i];

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
    }

    let mut sorted_stations = BTreeMap::new();
    for i in 0..CAP {
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

#[inline(always)]
unsafe fn process_chunk(mut ptr: *const u8, len: usize) -> FastMap {
    let mut map = FastMap::new();
    let end = ptr.add(len);

    while ptr < end {
        let mut name_hash = (ptr as *const u64).read_unaligned();
        let mut semi_pos;

        let input = name_hash ^ 0x3B3B3B3B3B3B3B3B;
        let tmp = (input.wrapping_sub(0x0101010101010101)) & (!input) & 0x8080808080808080;

        if tmp != 0 {
            semi_pos = (tmp.trailing_zeros() >> 3) as usize;
        } else {
            semi_pos = 8;
            while *ptr.add(semi_pos) != b';' {
                semi_pos += 1;
            }

            let last_word = (ptr.add(semi_pos - 8) as *const u64).read_unaligned();
            name_hash = name_hash.rotate_left(5) ^ last_word;
        }

        let entry = map.get_mut_or_create(ptr, semi_pos, name_hash);

        ptr = ptr.add(semi_pos + 1);
        let num_word = (ptr as *const u64).read_unaligned();

        let val: i16;
        let step: usize;

        let neg = (num_word & 0xFF) == 0x2D;

        if !neg {
            if ((num_word >> 8) & 0xFF) == 0x2E {
                val = (((num_word & 0xF) * 10) + ((num_word >> 16) & 0xF)) as i16;
                step = 4;
            } else {
                val = (((num_word & 0xF) * 100)
                    + (((num_word >> 8) & 0xF) * 10)
                    + ((num_word >> 24) & 0xF)) as i16;
                step = 5;
            }
        } else {
            let s = num_word >> 8;
            if ((s >> 8) & 0xFF) == 0x2E {
                val = -(((s & 0xF) * 10 + ((s >> 16) & 0xF)) as i16);
                step = 5;
            } else {
                val = -(((s & 0xF) * 100 + ((s >> 8) & 0xF) * 10 + ((s >> 24) & 0xF)) as i16);
                step = 6;
            }
        }

        entry.count += 1;
        entry.sum += val as i64;
        if val < entry.min {
            entry.min = val;
        }
        if val > entry.max {
            entry.max = val;
        }

        ptr = ptr.add(step);
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
