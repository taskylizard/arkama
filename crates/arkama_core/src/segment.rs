use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Segment {
    pub(crate) id: usize,
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) downloaded: u64,
}

impl Segment {
    pub(crate) fn remaining_start(&self) -> Option<u64> {
        let start = self.start + self.downloaded;
        if start > self.end {
            return None;
        }
        Some(start)
    }
}

pub(crate) fn build_segments(total: u64, connections: usize) -> Vec<Segment> {
    let mut segments = Vec::new();
    if total == 0 {
        return segments;
    }
    let max_connections = usize::try_from(total).unwrap_or(connections);
    let connections = if connections == 0 { 1 } else { connections };
    let connections = std::cmp::min(connections, max_connections);
    let base = total / connections as u64;
    let mut start = 0u64;
    let mut id = 0usize;
    while id < connections {
        let mut end = start + base;
        if id == connections - 1 {
            end = total - 1;
        } else {
            end -= 1;
        }
        segments.push(Segment {
            id,
            start,
            end,
            downloaded: 0,
        });
        start = end + 1;
        id += 1;
    }
    segments
}

pub(crate) fn build_segments_with_chunk_size(total: u64, chunk_size: u64) -> Vec<Segment> {
    let mut segments = Vec::new();
    if total == 0 {
        return segments;
    }
    let chunk_size = if chunk_size == 0 { 1 } else { chunk_size };
    let mut start = 0u64;
    let mut id = 0usize;
    while start < total {
        let mut end = start + chunk_size - 1;
        if end >= total {
            end = total - 1;
        }
        segments.push(Segment {
            id,
            start,
            end,
            downloaded: 0,
        });
        start = end + 1;
        id += 1;
    }
    segments
}

#[cfg(test)]
mod tests {
    use super::{Segment, build_segments};

    #[test]
    fn test_remaining_start_some() {
        let segment = Segment {
            id: 0,
            start: 5,
            end: 10,
            downloaded: 3,
        };

        assert_eq!(segment.remaining_start(), Some(8));
    }

    #[test]
    fn test_remaining_start_none_when_complete() {
        let segment = Segment {
            id: 0,
            start: 0,
            end: 9,
            downloaded: 10,
        };

        assert_eq!(segment.remaining_start(), None);
    }

    #[test]
    fn test_build_segments_zero_total() {
        let segments = build_segments(0, 4);
        assert!(segments.is_empty());
    }

    #[test]
    fn test_build_segments_zero_connections_defaults_to_one() {
        let segments = build_segments(10, 0);
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].start, 0);
        assert_eq!(segments[0].end, 9);
    }

    #[test]
    fn test_build_segments_total_less_than_connections() {
        let segments = build_segments(1, 4);
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].start, 0);
        assert_eq!(segments[0].end, 0);
    }

    #[test]
    fn test_build_segments_even_split() {
        let segments = build_segments(10, 3);
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].start, 0);
        assert_eq!(segments[0].end, 2);
        assert_eq!(segments[1].start, 3);
        assert_eq!(segments[1].end, 5);
        assert_eq!(segments[2].start, 6);
        assert_eq!(segments[2].end, 9);
    }
}
