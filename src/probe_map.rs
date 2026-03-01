pub const CAP: usize = 16384;
const MASK: usize = CAP - 1;

#[derive(Debug, Clone, Copy)]
pub struct StationData {
    pub min: i16,
    pub max: i16,
    pub count: u32,
    pub sum: i64,
}

impl Default for StationData {
    #[inline(always)]
    fn default() -> Self {
        Self {
            min: i16::MAX,
            max: i16::MIN,
            count: 0,
            sum: 0,
        }
    }
}

pub struct FastMap {
    pub keys: Vec<usize>,
    pub lens: Vec<usize>,
    pub values: Vec<StationData>,
}

impl FastMap {
    pub fn new() -> Self {
        Self {
            keys: vec![0; CAP],
            lens: vec![0; CAP],
            values: vec![StationData::default(); CAP],
        }
    }

    #[inline(always)]
    pub unsafe fn get_mut_or_create(
        &mut self,
        key_ptr: *const u8,
        len: usize,
        hash: u64,
    ) -> &mut StationData {
        let mut idx = (hash as usize) & MASK;

        loop {
            let k_ptr = *self.keys.get_unchecked(idx);

            if k_ptr == 0 {
                *self.keys.get_unchecked_mut(idx) = key_ptr as usize;
                *self.lens.get_unchecked_mut(idx) = len;
                return self.values.get_unchecked_mut(idx);
            }

            if *self.lens.get_unchecked(idx) == len {
                let stored_ptr = k_ptr as *const u8;
                if self.memcmp(stored_ptr, key_ptr, len) {
                    return self.values.get_unchecked_mut(idx);
                }
            }

            idx = (idx + 1) & MASK;
        }
    }

    #[inline(always)]
    unsafe fn memcmp(&self, p1: *const u8, p2: *const u8, len: usize) -> bool {
        let mut i = 0;
        while i < len {
            if *p1.add(i) != *p2.add(i) {
                return false;
            }
            i += 1;
        }
        true
    }
}

