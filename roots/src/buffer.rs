use std::{
    borrow::Cow,
    cmp::Ordering,
    fmt::{Debug, Write},
    io::{self, ErrorKind, Read},
    ops::Deref,
    sync::Arc,
};

use ranges::Domain;

/// A wrapper around a `Cow<'a, [u8]>` wrapper that implements Read, and has a
/// convenience method to take a slice of bytes as another Buffer which shares a
/// reference to the same underlying `Cow`.
#[derive(Clone)]
pub struct Buffer<'a> {
    buffer: Arc<Cow<'a, [u8]>>,
    end: usize,
    position: usize,
}

impl<'a> Debug for Buffer<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut slice = self.as_slice();
        write!(f, "Buffer {{ length: {}, bytes: [", slice.len())?;
        while !slice.is_empty() {
            let (chunk, remaining) = slice.split_at(4.min(slice.len()));
            slice = remaining;
            for byte in chunk {
                write!(f, "{:02x}", byte)?;
            }
            if !slice.is_empty() {
                f.write_char(' ')?;
            }
        }
        f.write_str("] }}")
    }
}

impl<'a> Eq for Buffer<'a> {}

impl<'a, 'b> PartialEq<Buffer<'b>> for Buffer<'a> {
    fn eq(&self, other: &Buffer<'b>) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'a> Ord for Buffer<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        if Arc::ptr_eq(&self.buffer, &other.buffer) {
            if self.position == other.position && self.end == other.end {
                Ordering::Equal
            } else {
                (&**self).cmp(&**other)
            }
        } else {
            (&**self).cmp(&**other)
        }
    }
}

impl<'a, 'b> PartialOrd<Buffer<'b>> for Buffer<'a> {
    fn partial_cmp(&self, other: &Buffer<'b>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Buffer<'a> {
    /// Converts this buffer into its slice and returns a static-lifetimed
    /// instance.
    #[must_use]
    pub fn to_owned(&self) -> Buffer<'static> {
        Buffer::from(self.as_slice().to_vec())
    }

    /// Reads `count` bytes from the front of the buffer, returning a buffer
    /// that shares the same underlying buffer.
    pub fn read_bytes(&mut self, count: usize) -> Result<Self, std::io::Error> {
        let start = self.position;
        let end = self.position + count;
        if end > self.end {
            Err(std::io::Error::from(ErrorKind::UnexpectedEof))
        } else {
            self.position = end;
            Ok(Self {
                buffer: self.buffer.clone(),
                end,
                position: start,
            })
        }
    }

    /// Returns this buffer as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer[self.position..self.end]
    }
}

impl<'a> From<Vec<u8>> for Buffer<'a> {
    fn from(buffer: Vec<u8>) -> Self {
        Self {
            end: buffer.len(),
            buffer: Arc::new(Cow::Owned(buffer)),
            position: 0,
        }
    }
}

impl<'a> From<&'a [u8]> for Buffer<'a> {
    fn from(buffer: &'a [u8]) -> Self {
        Self {
            end: buffer.len(),
            buffer: Arc::new(Cow::Borrowed(buffer)),
            position: 0,
        }
    }
}

impl<'a, const N: usize> From<&'a [u8; N]> for Buffer<'a> {
    fn from(buffer: &'a [u8; N]) -> Self {
        Self {
            end: buffer.len(),
            buffer: Arc::new(Cow::Borrowed(buffer)),
            position: 0,
        }
    }
}

impl<'a, const N: usize> From<[u8; N]> for Buffer<'a> {
    fn from(buffer: [u8; N]) -> Self {
        Self {
            end: buffer.len(),
            buffer: Arc::new(Cow::Owned(buffer.to_vec())),
            position: 0,
        }
    }
}

impl<'a> Deref for Buffer<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<'a> Read for Buffer<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let end = self.buffer.len().min(self.position + buf.len());
        let bytes_read = end - self.position;

        if bytes_read == 0 {
            return Err(io::Error::from(ErrorKind::UnexpectedEof));
        }

        buf.copy_from_slice(&self.buffer[self.position..end]);
        self.position = end;

        Ok(bytes_read)
    }
}

impl<'a> Domain for Buffer<'a> {
    const DISCRETE: bool = true;
}
