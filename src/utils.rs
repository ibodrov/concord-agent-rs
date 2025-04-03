use std::{
    fs::Permissions,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use anyhow::{Context, anyhow};
use async_zip::{
    Compression, ZipEntryBuilder,
    tokio::{read::seek::ZipFileReader, write::ZipFileWriter},
};
use futures_util::AsyncWriteExt;
use tokio::{
    fs::{self, File, OpenOptions, create_dir_all},
    io::{AsyncReadExt, BufReader},
};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::warn;
use walkdir::WalkDir;

fn sanitize_file_path(path: &str) -> PathBuf {
    path.replace('\\', "/")
        .split('/') // TODO sanitize components
        .collect()
}

pub async fn zip_directory(source_dir: &PathBuf, archive_file: &PathBuf) -> anyhow::Result<()> {
    // Ensure source directory exists and is a directory
    if !source_dir.is_dir() {
        anyhow::bail!("Source path is not a directory");
    }

    let output_file = File::create(&archive_file)
        .await
        .context("Failed to create output zip file")?;
    let mut zip = ZipFileWriter::with_tokio(output_file);

    let mut buffer = vec![0; 64 * 1024];
    for entry in WalkDir::new(source_dir) {
        let entry = entry.context("Failed to read directory entry")?;
        let path = entry.path();

        if path == source_dir {
            continue;
        }

        let relative_path = path
            .strip_prefix(source_dir)
            .context("Failed to calculate relative path")?;

        let metadata = fs::metadata(path)
            .await
            .context("Failed to read metadata")?;

        if metadata.is_file() {
            let mode = metadata.permissions().mode();

            let mut entry_builder = ZipEntryBuilder::new(
                relative_path.to_string_lossy().into_owned().into(),
                Compression::Deflate,
            );

            entry_builder = entry_builder.unix_permissions(mode as u16);

            let mut entry_writer = zip
                .write_entry_stream(entry_builder)
                .await
                .context("Failed to start zip entry")?;

            let mut file = File::open(path)
                .await
                .context("Failed to open file for reading")?;

            loop {
                buffer.clear();

                let bytes_read = file
                    .read(&mut buffer)
                    .await
                    .context("Failed to read from file")?;

                if bytes_read == 0 {
                    break;
                }

                entry_writer
                    .write_all(&buffer[..bytes_read])
                    .await
                    .context("Failed to write to zip entry")?;
            }

            entry_writer
                .close()
                .await
                .context("Failed to finish zip entry")?;
        } else if metadata.is_dir() {
            let mode = metadata.permissions().mode();

            let mut dir_path = relative_path.to_string_lossy().into_owned();
            if !dir_path.ends_with('/') {
                dir_path.push('/');
            }

            let mut entry_builder = ZipEntryBuilder::new(
                dir_path.into(),
                Compression::Stored, // Directories are typically stored without compression
            );

            entry_builder = entry_builder.unix_permissions(mode as u16);

            zip.write_entry_whole(entry_builder, &[])
                .await
                .context("Failed to write directory entry")?;
        } else {
            warn!(?metadata, "Skipping unsupported attachment file type")
        }
    }

    zip.close().await.context("Failed to finalize zip file")?;

    Ok(())
}

pub async fn unzip(archive_file: File, out_dir: &Path) -> anyhow::Result<()> {
    let archive = BufReader::new(archive_file).compat();

    let mut reader = ZipFileReader::new(archive).await?;

    let entry_count = reader.file().entries().len();
    let mut entries = Vec::with_capacity(entry_count);

    for (index, entry) in reader.file().entries().iter().enumerate() {
        let filename = entry.filename().to_owned();
        let permissions = entry.unix_permissions();
        entries.push((index, filename, permissions));
    }

    for (index, filename, permissions) in entries {
        let filename = sanitize_file_path(filename.as_str()?);
        let entry_is_dir = filename.ends_with("/");
        let path = out_dir.join(filename);

        if entry_is_dir {
            if !path.exists() {
                create_dir_all(&path)
                    .await
                    .with_context(|| format!("Failed to create directory: {path:?}"))?;
            }
        } else {
            let mut entry_reader = reader.reader_without_entry(index).await?;

            let parent = path
                .parent()
                .ok_or_else(|| anyhow!("Failed to get parent path of the target file: {path:?}"))?;
            if !parent.is_dir() {
                create_dir_all(parent)
                    .await
                    .with_context(|| format!("Failed to create parent directories: {path:?}"))?;
            }

            let writer = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .await
                .with_context(|| format!("Failed to open file for writing: {path:?}"))?;

            futures_util::io::copy(&mut entry_reader, &mut writer.compat_write())
                .await
                .context("Failed to copy ZipEntry data")?;

            #[cfg(unix)]
            {
                if let Some(permissions) = permissions {
                    let permissions = Permissions::from_mode(permissions as u32);
                    fs::set_permissions(&path, permissions)
                        .await
                        .with_context(|| format!("Failed to set permissions for {path:?}"))?;
                }
            }
        }
    }

    Ok(())
}
