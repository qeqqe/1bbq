use core::f64;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{BufRead, BufReader},
    str::from_utf8,
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
    let mut stations: HashMap<Vec<u8>, StationData> = HashMap::new();

    let file = File::open("measurements/measurements.txt").unwrap();

    let reader = BufReader::new(file);

    for line in reader.split(b'\n') {
        let line = line.unwrap();
        let mut fields = line.splitn(2, |c| *c == b';');
        let station = fields.next().unwrap();
        let temp = fields.next().unwrap();
        let temp: f64 = from_utf8(temp).unwrap().parse().unwrap();
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
            .map(|(k, v)| (String::from_utf8(k).unwrap(), v)),
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
