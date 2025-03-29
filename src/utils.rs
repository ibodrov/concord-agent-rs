use std::{
    fs::Permissions,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use anyhow::{Context, anyhow};
use tokio::{
    fs::{self, File, OpenOptions, create_dir_all},
    io::BufReader,
};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

fn sanitize_file_path(path: &str) -> PathBuf {
    path.replace('\\', "/")
        .split('/') // TODO sanitize components
        .collect()
}

pub async fn unzip(archive_file: File, out_dir: &Path) -> anyhow::Result<()> {
    let archive = BufReader::new(archive_file).compat();

    let mut reader = async_zip::tokio::read::seek::ZipFileReader::new(archive).await?;

    let entries = reader
        .file()
        .entries()
        .iter()
        .map(|e| (e.filename().clone(), e.unix_permissions()))
        .enumerate()
        .collect::<Box<[_]>>();

    for (index, (filename, permissions)) in entries {
        let filename = sanitize_file_path(filename.as_str()?);
        let entry_is_dir = filename.ends_with("/");
        let path = out_dir.join(filename);

        if entry_is_dir {
            if !path.exists() {
                create_dir_all(&path).await?;
            }
        } else {
            let mut entry_reader = reader.reader_without_entry(index).await?;

            let parent = path.parent().ok_or(anyhow!(
                "Failed to get parent path of the target file: {path:?}"
            ))?;
            if !parent.is_dir() {
                create_dir_all(parent)
                    .await
                    .context("Failed to create parent directories")?;
            }

            let writer = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .await
                .context("Failed to open file for writing")?;

            futures_util::io::copy(&mut entry_reader, &mut writer.compat_write())
                .await
                .context("Failed to copy ZipEntry data")?;

            #[cfg(unix)]
            {
                if let Some(permissions) = permissions {
                    let permissions = Permissions::from_mode(permissions as u32);
                    fs::set_permissions(&path, permissions).await?;
                }
            }
        }
    }

    Ok(())
}
