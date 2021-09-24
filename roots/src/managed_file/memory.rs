use std::{
    collections::HashMap,
    convert::TryFrom,
    io::{self, SeekFrom},
    ops::Neg,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};

use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};

use super::{FileManager, FileOp, ManagedFile, OpenableFile};
use crate::Error;

/// A fake "file" represented by an in-memory buffer. This should only be used
/// in testing, as this database format is not optimized for memory efficiency.
#[derive(Debug)]
pub struct MemoryFile {
    path: Arc<PathBuf>,
    buffer: Arc<RwLock<Vec<u8>>>,
    position: usize,
}

type OpenBuffers = Arc<Mutex<HashMap<PathBuf, Weak<RwLock<Vec<u8>>>>>>;
static OPEN_BUFFERS: Lazy<OpenBuffers> = Lazy::new(Arc::default);

#[allow(clippy::needless_pass_by_value)]
fn lookup_buffer(
    path: impl AsRef<std::path::Path> + Send,
    create_if_not_found: bool,
) -> Option<Arc<RwLock<Vec<u8>>>> {
    let mut open_buffers = OPEN_BUFFERS.lock();
    if let Some(existing_buffer) = open_buffers.get(path.as_ref()).and_then(Weak::upgrade) {
        Some(existing_buffer)
    } else if create_if_not_found {
        let new_buffer = Arc::default();
        open_buffers.insert(path.as_ref().to_path_buf(), Arc::downgrade(&new_buffer));
        Some(new_buffer)
    } else {
        None
    }
}

#[allow(clippy::cast_possible_truncation)]
impl ManagedFile for MemoryFile {
    type Manager = MemoryFileManager;
    fn path(&self) -> Arc<PathBuf> {
        self.path.clone()
    }

    fn open_for_read(path: impl AsRef<std::path::Path> + Send) -> Result<Self, Error> {
        let path = path.as_ref();
        Ok(Self {
            path: Arc::new(path.to_path_buf()),
            buffer: lookup_buffer(path, true).unwrap(),
            position: 0,
        })
    }

    fn open_for_append(path: impl AsRef<std::path::Path> + Send) -> Result<Self, Error> {
        let path = path.as_ref();
        let buffer = lookup_buffer(path, true).unwrap();
        let position = {
            let buffer = buffer.read();
            buffer.len()
        };
        Ok(Self {
            path: Arc::new(path.to_path_buf()),
            buffer,
            position,
        })
    }

    fn close(self) -> Result<(), Error> {
        Ok(())
    }
}

impl std::io::Seek for MemoryFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Start(position) => self.position = usize::try_from(position).unwrap(),
            SeekFrom::End(from_end) => {
                let buffer = self.buffer.read();
                self.position = if from_end.is_positive() {
                    buffer.len()
                } else {
                    buffer.len() - usize::try_from(from_end.neg()).unwrap()
                };
            }
            SeekFrom::Current(relative) => {
                self.position = if relative.is_positive() {
                    self.position + usize::try_from(relative).unwrap()
                } else {
                    self.position - usize::try_from(relative.neg()).unwrap()
                }
            }
        }
        Ok(self.position as u64)
    }
}

impl std::io::Read for MemoryFile {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let file_buffer = self.buffer.read();

        let read_end = self.position as usize + buffer.len();
        if read_end > file_buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                Error::message("read requested more bytes than available"),
            ));
        }

        buffer.copy_from_slice(&file_buffer[self.position..read_end]);
        self.position = read_end;

        Ok(buffer.len())
    }
}

impl std::io::Write for MemoryFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut file_buffer = self.buffer.write();

        file_buffer.extend_from_slice(buf);
        self.position += buf.len();

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct MemoryFileManager {
    open_files: Arc<Mutex<HashMap<PathBuf, Arc<Mutex<MemoryFile>>>>>,
}

impl MemoryFileManager {
    fn lookup_file(
        &self,
        path: impl AsRef<Path>,
        create_if_needed: bool,
    ) -> Result<Option<Arc<Mutex<MemoryFile>>>, Error> {
        let mut open_files = self.open_files.lock();
        if let Some(open_file) = open_files.get(path.as_ref()) {
            Ok(Some(open_file.clone()))
        } else if create_if_needed {
            let file = Arc::new(Mutex::new(MemoryFile::open_for_append(path.as_ref())?));
            open_files.insert(path.as_ref().to_path_buf(), file.clone());
            Ok(Some(file))
        } else {
            Ok(None)
        }
    }
}

impl FileManager for MemoryFileManager {
    type File = MemoryFile;
    type FileHandle = OpenMemoryFile;

    fn append(&self, path: impl AsRef<Path> + Send) -> Result<Self::FileHandle, Error> {
        self.lookup_file(path, true)
            .map(|file| OpenMemoryFile(file.unwrap()))
    }

    fn read(&self, path: impl AsRef<Path> + Send) -> Result<Self::FileHandle, Error> {
        self.append(path)
    }

    fn file_length(&self, path: impl AsRef<Path> + Send) -> Result<u64, Error> {
        let file = self.lookup_file(path, false)?.ok_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                Error::message("not found"),
            ))
        })?;
        let file = file.lock();
        let buffer = file.buffer.read();
        Ok(buffer.len() as u64)
    }

    fn exists(&self, path: impl AsRef<Path> + Send) -> Result<bool, Error> {
        Ok(self.lookup_file(path, false)?.is_some())
    }

    fn delete(&self, path: impl AsRef<Path> + Send) -> Result<bool, Error> {
        let path = path.as_ref();
        let mut open_files = self.open_files.lock();
        Ok(open_files.remove(path).is_some())
    }
}

pub struct OpenMemoryFile(Arc<Mutex<MemoryFile>>);

impl OpenableFile<MemoryFile> for OpenMemoryFile {
    fn execute<W: FileOp<MemoryFile>>(&mut self, mut writer: W) -> W::Output {
        let mut file = self.0.lock();
        writer.execute(&mut file)
    }

    fn close(self) -> Result<(), Error> {
        drop(self);
        Ok(())
    }
}
