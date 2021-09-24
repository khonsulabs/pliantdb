use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::Mutex;

use super::{FileManager, FileOp, ManagedFile, OpenableFile};
use crate::Error;

/// An open file that uses [`tokio::fs`].
#[derive(Debug)]
pub struct StdFile {
    file: File,
    path: Arc<PathBuf>,
}

impl ManagedFile for StdFile {
    type Manager = StdFileManager;
    fn path(&self) -> Arc<PathBuf> {
        self.path.clone()
    }

    fn open_for_read(path: impl AsRef<std::path::Path> + Send) -> Result<Self, Error> {
        let path = path.as_ref();
        Ok(Self {
            file: File::open(path)?,
            path: Arc::new(path.to_path_buf()),
        })
    }

    fn open_for_append(path: impl AsRef<std::path::Path> + Send) -> Result<Self, Error> {
        let path = path.as_ref();
        Ok(Self {
            file: OpenOptions::new()
                .write(true)
                .append(true)
                .read(true)
                .create(true)
                .open(path)?,
            path: Arc::new(path.to_path_buf()),
        })
    }

    fn close(mut self) -> Result<(), Error> {
        // Closing is done by just dropping it
        self.flush().map_err(Error::from)
    }
}

impl Seek for StdFile {
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self,)))]
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

impl Write for StdFile {
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, buf)))]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl Read for StdFile {
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, buf)))]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

#[derive(Debug, Default, Clone)]
pub struct StdFileManager {
    open_files: Arc<Mutex<HashMap<PathBuf, Arc<Mutex<StdFile>>>>>,
}

impl FileManager for StdFileManager {
    type File = StdFile;
    type FileHandle = OpenStdFile;
    fn append(&self, path: impl AsRef<Path> + Send) -> Result<Self::FileHandle, Error> {
        let mut open_files = self.open_files.lock();
        if let Some(open_file) = open_files.get(path.as_ref()) {
            Ok(OpenStdFile(open_file.clone()))
        } else {
            let file = Arc::new(Mutex::new(StdFile::open_for_append(path.as_ref())?));
            open_files.insert(path.as_ref().to_path_buf(), file.clone());
            Ok(OpenStdFile(file))
        }
    }

    fn read(&self, path: impl AsRef<Path> + Send) -> Result<Self::FileHandle, Error> {
        // TODO we should come up with a way to cache open files. We want to
        // support more than one open reader for a file at any given time, but
        // we should be able to limit the number of open files. A time+capacity
        // based Lru cache might work.
        let file = Arc::new(Mutex::new(StdFile::open_for_read(path)?));
        Ok(OpenStdFile(file))
    }
}

pub struct OpenStdFile(Arc<Mutex<StdFile>>);

impl OpenableFile<StdFile> for OpenStdFile {
    fn execute<W: FileOp<StdFile>>(&mut self, mut writer: W) -> W::Output {
        let mut file = self.0.lock();
        writer.execute(&mut file)
    }

    fn close(self) -> Result<(), Error> {
        drop(self);
        Ok(())
    }
}
