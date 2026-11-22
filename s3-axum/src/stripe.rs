//! How an object's bytes are laid out across drives (identical on-disk format
//! to `multiring-s3` so both runtimes interoperate on the same directories).

use std::ops::Range;

use tokio_multiring::Segment;

pub(crate) const BLOCK: usize = 4096;
pub(crate) const HEADER: usize = BLOCK;
/// Preserved from the crate's original name for on-disk compatibility.
const MAGIC: &[u8; 8] = b"sidecar1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Geometry {
    drives: usize,
    unit: usize,
    chunk: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    pub(crate) reads: Vec<Option<Read>>,
    pub(crate) segments: Vec<Segment>,
    pub(crate) len: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Read {
    pub(crate) offset: u64,
    pub(crate) limit: usize,
    pub(crate) needed: usize,
}

struct Piece {
    part: usize,
    offset: u64,
    len: usize,
}

impl Geometry {
    pub(crate) fn new(drives: usize, unit: usize, chunk: usize) -> Option<Geometry> {
        let valid = drives > 0
            && unit > 0
            && unit % BLOCK == 0
            && chunk > 0
            && chunk % unit == 0
            && chunk.checked_mul(drives).is_some();
        valid.then_some(Geometry {
            drives,
            unit,
            chunk,
        })
    }

    pub(crate) fn drives(&self) -> usize {
        self.drives
    }

    pub(crate) fn unit(&self) -> usize {
        self.unit
    }

    pub(crate) fn chunk(&self) -> usize {
        self.chunk
    }

    pub(crate) fn round_len(&self) -> usize {
        self.drives * self.chunk
    }

    pub(crate) fn parts(&self, size: u64) -> usize {
        let units = size.div_ceil(self.unit as u64);
        units.clamp(1, self.drives as u64) as usize
    }

    pub(crate) fn part_len(&self, size: u64, part: usize) -> u64 {
        let (unit, drives, part) = (self.unit as u64, self.drives as u64, part as u64);
        let whole_units = size / unit;
        let mut units = whole_units / drives;
        if part < whole_units % drives {
            units += 1;
        }
        let tail = if part == whole_units % drives {
            size % unit
        } else {
            0
        };
        units * unit + tail
    }

    fn pieces(&self, range: Range<u64>) -> impl Iterator<Item = Piece> + use<> {
        let (unit, drives) = (self.unit as u64, self.drives as u64);
        let mut position = range.start;
        std::iter::from_fn(move || {
            if position >= range.end {
                return None;
            }
            let (index, within) = (position / unit, position % unit);
            let len = (unit - within).min(range.end - position);
            position += len;
            Some(Piece {
                part: (index % drives) as usize,
                offset: index / drives * unit + within,
                len: len as usize,
            })
        })
    }

    pub(crate) fn rounds(&self, range: &Range<u64>) -> Range<u64> {
        if range.is_empty() {
            return 0..0;
        }
        let round_len = self.round_len() as u64;
        range.start / round_len..range.end.div_ceil(round_len)
    }

    pub(crate) fn plan(&self, round: u64, range: &Range<u64>) -> Plan {
        let round_len = self.round_len() as u64;
        let wanted = range.start.max(round * round_len)..range.end.min((round + 1) * round_len);

        let mut touched: Vec<Option<Range<u64>>> = vec![None; self.drives];
        for piece in self.pieces(wanted.clone()) {
            let end = piece.offset + piece.len as u64;
            let start = touched[piece.part]
                .take()
                .map_or(piece.offset, |span| span.start);
            touched[piece.part] = Some(start..end);
        }
        let reads: Vec<Option<Read>> = touched
            .iter()
            .map(|span| {
                let span = span.as_ref()?;
                let offset = span.start - span.start % BLOCK as u64;
                let needed = (span.end - offset) as usize;
                Some(Read {
                    offset,
                    limit: needed.next_multiple_of(BLOCK),
                    needed,
                })
            })
            .collect();

        let mut len = 0;
        let segments = self
            .pieces(wanted)
            .map(|piece| {
                let read = reads[piece.part].expect("every piece is in a touched part");
                len += piece.len;
                Segment {
                    buf: piece.part,
                    start: (piece.offset - read.offset) as usize,
                    len: piece.len,
                }
            })
            .collect();
        Plan {
            reads,
            segments,
            len,
        }
    }

    pub(crate) fn fill(&self, range: Range<usize>) -> Vec<Segment> {
        self.pieces(range.start as u64..range.end as u64)
            .map(|piece| Segment {
                buf: piece.part,
                start: piece.offset as usize,
                len: piece.len,
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) size: u64,
    pub(crate) modified: (i64, u32),
    pub(crate) id: u128,
}

impl Header {
    pub(crate) fn encode(&self, geometry: &Geometry) -> [u8; HEADER] {
        let mut block = [0; HEADER];
        let mut fields = block.chunks_exact_mut(8);
        let mut put = |field: [u8; 8]| fields.next().expect("room").copy_from_slice(&field);
        put(*MAGIC);
        put((geometry.drives as u64).to_le_bytes());
        put((geometry.unit as u64).to_le_bytes());
        put(self.size.to_le_bytes());
        put(self.modified.0.to_le_bytes());
        put(u64::from(self.modified.1).to_le_bytes());
        put((self.id as u64).to_le_bytes());
        put(((self.id >> 64) as u64).to_le_bytes());
        block
    }

    pub(crate) fn decode(block: &[u8], geometry: &Geometry) -> Option<Header> {
        if block.len() < HEADER {
            return None;
        }
        let mut fields = block.chunks_exact(8).map(|field| {
            u64::from_le_bytes(field.try_into().expect("chunks are eight bytes long"))
        });
        let mut next = || fields.next().expect("a block holds every field");
        let same_layout = next() == u64::from_le_bytes(*MAGIC)
            && next() == geometry.drives as u64
            && next() == geometry.unit as u64;
        if !same_layout {
            return None;
        }
        let size = next();
        let modified = (next() as i64, u32::try_from(next()).ok()?);
        let id = u128::from(next()) | u128::from(next()) << 64;
        Some(Header { size, modified, id })
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Meta {
    pub(crate) size: u64,
    pub(crate) modified: (i64, u32),
}

#[derive(Debug)]
pub(crate) struct ListedBucket {
    pub(crate) name: String,
    pub(crate) modified_secs: i64,
}

#[derive(Debug)]
pub(crate) struct ListedObject {
    pub(crate) key: String,
    pub(crate) meta: Meta,
}

#[derive(Debug)]
pub(crate) struct ListPage {
    pub(crate) contents: Vec<ListedObject>,
    pub(crate) is_truncated: bool,
    pub(crate) next_continuation_token: Option<String>,
}
