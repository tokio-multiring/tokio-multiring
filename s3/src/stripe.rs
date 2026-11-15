//! How an object's bytes are laid out across drives.
//!
//! An object is cut into units of a fixed size, dealt out in turn to as many
//! parts as there are drives: unit 0 to part 0, unit 1 to part 1, and so on
//! around. Each part is one file on one drive, holding its units back to
//! back. Part 0 lives on the drive a hash of the object's name picks, part 1
//! on the next drive, wrapping around.
//!
//! Part 0 starts with a [`Header`], one block long, which says how large the
//! object is and how its other parts are named. An object no larger than one
//! unit has no other parts.
//!
//! Transfers move through one buffer per part. A round is what those buffers
//! hold together: the same stretch of every part, which is a contiguous
//! stretch of the object. Reading or writing a round is one large operation
//! per drive, and the units only alternate on the socket.

use std::ops::Range;

use tokio_sidecar::Segment;

/// Offsets and lengths of direct I/O are multiples of this. It is a multiple
/// of every logical block size in use.
pub(crate) const BLOCK: usize = 4096;

/// Space at the start of part 0 that the header takes.
pub(crate) const HEADER: usize = BLOCK;

const MAGIC: &[u8; 8] = b"sidecar1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Geometry {
    drives: usize,
    unit: usize,
    /// Bytes of one part that take part in a round: the size of a buffer.
    chunk: usize,
}

/// What to read for the bytes of an object that fall into one round.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    /// What to read from each part.
    pub(crate) reads: Vec<Option<Read>>,
    /// The bytes in object order: regions of the buffers the reads fill, the
    /// buffer of part `n` being number `n`.
    pub(crate) segments: Vec<Segment>,
    /// How many bytes that is.
    pub(crate) len: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Read {
    /// Where in the part to start, a multiple of [`BLOCK`]. In part 0 this
    /// counts from the end of the header.
    pub(crate) offset: u64,
    /// How much to ask for, a multiple of [`BLOCK`].
    pub(crate) limit: usize,
    /// How much of that has to be there.
    pub(crate) needed: usize,
}

/// A stretch of one unit.
struct Piece {
    part: usize,
    /// Where in the part it starts.
    offset: u64,
    len: usize,
}

impl Geometry {
    /// A layout over `drives` drives in units of `unit` bytes, moved through
    /// buffers of `chunk` bytes. `None` unless there is a drive, the unit is
    /// a whole number of blocks and the buffer a whole number of units.
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

    /// Bytes of one part that take part in a round.
    pub(crate) fn chunk(&self) -> usize {
        self.chunk
    }

    /// Bytes of an object that make up a round.
    pub(crate) fn round_len(&self) -> usize {
        self.drives * self.chunk
    }

    /// How many parts an object of `size` bytes has. Even an empty one has
    /// part 0, for its header.
    pub(crate) fn parts(&self, size: u64) -> usize {
        let units = size.div_ceil(self.unit as u64);
        units.clamp(1, self.drives as u64) as usize
    }

    /// How many of the first `size` bytes of an object are in part `part`.
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

    /// Cuts the bytes `range` of an object at unit boundaries.
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

    /// The rounds that the bytes `range` of an object fall into.
    pub(crate) fn rounds(&self, range: &Range<u64>) -> Range<u64> {
        if range.is_empty() {
            return 0..0;
        }
        let round_len = self.round_len() as u64;
        range.start / round_len..range.end.div_ceil(round_len)
    }

    /// What to read to get the bytes of `range` that fall into round `round`.
    pub(crate) fn plan(&self, round: u64, range: &Range<u64>) -> Plan {
        let round_len = self.round_len() as u64;
        let wanted = range.start.max(round * round_len)..range.end.min((round + 1) * round_len);

        // The stretch of each part that is touched. Within a round a part's
        // bytes come in order, so this is one stretch.
        let mut touched: Vec<Option<Range<u64>>> = vec![None; self.drives];
        for piece in self.pieces(wanted.clone()) {
            let end = piece.offset + piece.len as u64;
            let start = touched[piece.part]
                .take()
                .map_or(piece.offset, |span| span.start);
            touched[piece.part] = Some(start..end);
        }
        // Reads start on a block boundary, which may be short of the stretch.
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

    /// Where the bytes `range` of a round go in the round's buffers, for
    /// filling them from the start of each.
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

/// What the first block of part 0 says about its object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Header {
    /// Size of the object in bytes.
    pub(crate) size: u64,
    /// When the object was stored, as seconds and nanoseconds since the Unix
    /// epoch.
    pub(crate) modified: (i64, u32),
    /// Names the object's other parts.
    pub(crate) id: u128,
}

impl Header {
    /// Writes the header's block.
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

    /// Reads a header's block. `None` if it is not one, or if the object was
    /// stored with a different layout than `geometry`, by which its parts
    /// could not be put back together.
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

#[cfg(test)]
mod tests {
    use super::*;

    const UNIT: usize = 2 * BLOCK;

    fn geometry(drives: usize) -> Geometry {
        // Three units to a buffer.
        Geometry::new(drives, UNIT, 3 * UNIT).unwrap()
    }

    /// Where each byte of an object goes, worked out one byte at a time.
    fn naive(geometry: &Geometry, position: u64) -> (usize, u64) {
        let unit = position / UNIT as u64;
        let part = (unit % geometry.drives as u64) as usize;
        let offset = unit / geometry.drives as u64 * UNIT as u64 + position % UNIT as u64;
        (part, offset)
    }

    #[test]
    fn layouts_that_make_no_sense_are_refused() {
        assert!(Geometry::new(0, BLOCK, BLOCK).is_none());
        assert!(Geometry::new(1, 0, BLOCK).is_none());
        assert!(Geometry::new(1, BLOCK + 1, 4 * BLOCK).is_none());
        assert!(Geometry::new(1, 2 * BLOCK, 3 * BLOCK).is_none());
        assert!(Geometry::new(1, BLOCK, 0).is_none());
        assert!(Geometry::new(1, BLOCK, BLOCK).is_some());
        assert!(Geometry::new(3, 2 * BLOCK, 8 * BLOCK).is_some());
    }

    #[test]
    fn part_lengths_add_up() {
        for drives in 1..=4 {
            let geometry = geometry(drives);
            for size in (0..40 * UNIT as u64).step_by(BLOCK / 2 + 1) {
                let mut counted = vec![0u64; drives];
                for position in (0..size).step_by(61) {
                    let next = (position + 61).min(size);
                    for position in position..next {
                        counted[naive(&geometry, position).0] += 1;
                    }
                }
                for (part, &count) in counted.iter().enumerate() {
                    assert_eq!(
                        geometry.part_len(size, part),
                        count,
                        "{drives} {size} {part}"
                    );
                }
                let parts = geometry.parts(size);
                assert!(parts >= 1 && parts <= drives);
                for part in 0..drives {
                    let holds_data = geometry.part_len(size, part) > 0;
                    assert_eq!(
                        holds_data,
                        part < parts && size > 0,
                        "{drives} {size} {part}"
                    );
                }
            }
        }
    }

    /// A deterministic stand-in for random numbers.
    fn scrambled(seed: &mut u64) -> u64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    }

    /// Every plan, carried out against a model of the parts, yields exactly
    /// the bytes of the range, and reads only what the rules allow.
    #[test]
    fn plans_reassemble_the_object() {
        let mut seed = 0x9e37_79b9_7f4a_7c15;
        for drives in 1..=4 {
            let geometry = geometry(drives);
            for _ in 0..400 {
                let size = scrambled(&mut seed) % (30 * UNIT as u64) + 1;
                let start = scrambled(&mut seed) % size;
                let end = start + 1 + scrambled(&mut seed) % (size - start);
                let range = start..end;

                let mut position = start;
                for round in geometry.rounds(&range) {
                    let plan = geometry.plan(round, &range);
                    for (part, read) in plan.reads.iter().enumerate() {
                        let Some(read) = read else { continue };
                        assert_eq!(read.offset % BLOCK as u64, 0);
                        assert_eq!(read.limit % BLOCK, 0);
                        assert!(read.needed <= read.limit && read.limit <= geometry.chunk());
                        // Within the round's stretch of the part, and within
                        // the part.
                        let chunk = geometry.chunk() as u64;
                        assert!(read.offset >= round * chunk);
                        assert!(read.offset + read.limit as u64 <= (round + 1) * chunk);
                        let part_len = geometry.part_len(size, part);
                        assert!(read.offset + read.needed as u64 <= part_len);
                    }
                    let mut len = 0;
                    for segment in &plan.segments {
                        let read = plan.reads[segment.buf].unwrap();
                        assert!(segment.start + segment.len <= read.needed);
                        for byte in 0..segment.len as u64 {
                            let in_part = read.offset + segment.start as u64 + byte;
                            assert_eq!(naive(&geometry, position), (segment.buf, in_part));
                            position += 1;
                        }
                        len += segment.len;
                    }
                    assert_eq!(plan.len, len);
                    assert!(len > 0);
                }
                assert_eq!(position, end, "{drives} drives, {range:?} of {size}");
            }
        }
    }

    #[test]
    fn empty_ranges_need_nothing() {
        let geometry = geometry(2);
        assert!(geometry.rounds(&(0..0)).is_empty());
        assert!(geometry.rounds(&(5..5)).is_empty());
    }

    /// Filling a round's buffers puts every byte where the layout has it,
    /// and each buffer is filled from its start without gaps.
    #[test]
    fn filling_a_round_follows_the_layout() {
        for drives in 1..=4 {
            let geometry = geometry(drives);
            let round_len = geometry.round_len();
            for filled in [1, UNIT - 1, UNIT, UNIT + 1, round_len / 2 + 7, round_len] {
                // In two steps, as a transfer that arrives in pieces would.
                let split = filled / 3;
                let mut position = 0u64;
                let mut ends = vec![0; drives];
                for range in [0..split, split..filled] {
                    for segment in geometry.fill(range) {
                        assert_eq!(segment.start, ends[segment.buf], "a gap in a buffer");
                        ends[segment.buf] += segment.len;
                        let expected = naive(&geometry, position);
                        assert_eq!((segment.buf, segment.start as u64), expected);
                        position += segment.len as u64;
                    }
                }
                assert_eq!(position, filled as u64);
                for (part, &end) in ends.iter().enumerate() {
                    assert_eq!(end as u64, geometry.part_len(filled as u64, part));
                }
            }
        }
    }

    #[test]
    fn headers_round_trip() {
        let geometry = geometry(2);
        let header = Header {
            size: 0x0123_4567_89ab_cdef,
            modified: (-5, 999_999_999),
            id: 0xfeed_face_cafe_beef_0011_2233_4455_6677,
        };
        let block = header.encode(&geometry);
        assert_eq!(Header::decode(&block, &geometry), Some(header));
    }

    #[test]
    fn foreign_headers_are_refused() {
        let geometry = geometry(2);
        let header = Header {
            size: 1,
            modified: (0, 0),
            id: 7,
        };
        let block = header.encode(&geometry);
        assert_eq!(Header::decode(&block[..HEADER - 1], &geometry), None);
        assert_eq!(Header::decode(&[0; HEADER], &geometry), None);
        // Stored with another number of drives, or another unit.
        assert_eq!(
            Header::decode(&block, &super::Geometry::new(3, UNIT, 3 * UNIT).unwrap()),
            None
        );
        assert_eq!(
            Header::decode(&block, &Geometry::new(2, BLOCK, 3 * UNIT).unwrap()),
            None
        );
        let mut scribbled = block;
        scribbled[0] ^= 1;
        assert_eq!(Header::decode(&scribbled, &geometry), None);
    }
}
