use core::f64;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

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
    let mut stations: HashMap<String, StationData> = HashMap::new();

    let file = File::open("measurements/measurements.txt").unwrap();

    let reader = BufReader::new(file);

    for line in reader.lines().map(|l| l.unwrap()) {
        let (station, temp) = line.split_once(';').unwrap();
        let temp: f64 = temp.parse().unwrap();
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
                        total: 0.0,
                        min: temp,
                        max: temp,
                        accumulate: temp,
                    },
                );
            }
        }
    }

    for (station, stats) in stations {
        print!(
            "{{{station}={}/{:.1}/{:.1}}}, ",
            stats.min,
            stats.accumulate / stats.total,
            stats.max
        );
    }
}
