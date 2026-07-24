/// KLL quantile sketch (Karnin-Lang-Liberty), generic over `Ord` items.
pub struct KllSketch<T: Ord> {
    items: Vec<T>,
}

impl<T: Ord> KllSketch<T> {
    /// Construct an empty sketch.
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    /// Add one item to the sketch.
    pub fn insert(&mut self, x: T) {
        self.items.push(x);
    }

    /// Return the number of items in the sketch strictly less than `x`.
    pub fn rank(&self, x: &T) -> u64 {
        self.items.iter().filter(|y| *y < x).count() as u64
    }
}

impl<T: Ord> Default for KllSketch<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank_is_exact_below_capacity() {
        let mut sketch: KllSketch<u32> = KllSketch::new();
        for x in [7, 3, 10, 1, 5, 8, 2, 6, 9, 4] {
            sketch.insert(x);
        }
        // 4 items strictly less than 5: {1, 2, 3, 4}
        assert_eq!(sketch.rank(&5), 4);
    }
}
