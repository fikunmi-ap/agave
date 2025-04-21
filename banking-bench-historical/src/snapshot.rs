//! Provides utilities to download a valid Solana snapshot.

use {
    anyhow::Result,
    download::download_snapshot,
    std::{fs, io, path::{Path, PathBuf}},

};

/// Processes a snapshot and returns the path to the
/// processed snapshot.
/// 
/// - Checks first if there is a snapshot in `save_dir`.
///    - If there is, it returns that
///    - If there isn't, it downloads the latest
/// available snapshot from the snapshot URL.
///
///
/// NOTE: If you want to use an exisiting snapshot, the
/// `save_dir` path must contain a valid and appropriately
/// named snapshot file, current convention for naming
/// snapshot is `snapshot-<BLOCK HEIGHT>-<HASH>`.
/// Where `BLOCK HEIGHT` is the height at which the snapshot
/// was produced and `HASH` is the hash.
pub async fn process_snapshot(
    reqwest_client: &reqwest::Client,
    snapshot_url: &str,
    save_dir: impl AsRef<Path>,
) -> Result<PathBuf> {
    if let Ok(Some(snapshot_path)) = get_snapshot_from_cache(save_dir.as_ref()) {
        return Ok(snapshot_path);
    }

    let compressed_snapshot = download_snapshot(save_dir, reqwest_client, snapshot_url).await?;
    Ok(compressed_snapshot)
}

/// Finds the first file that starts with "snapshot-" within the given directory.
///
/// # Arguments
/// * `snapshot_dir` - The directory path to search for snapshot files
///
/// # Returns
/// * `Ok(Some(PathBuf))` - If a snapshot file is found
/// * `Ok(None)` - If no snapshot file is found
/// * `Err(io::Error)` - If there's an error accessing the directory.
/// 
/// TODO
/// * Implement freshness logic for the snapshot cache i.e., use the
/// latest snapshot in the cache and disregard snapshots that are too
/// old during the check process.
/// * Implement better verification for the snapshot file. Can verify
/// if it's actually a valid snapshot.
fn get_snapshot_from_cache(snapshot_dir: &Path) -> io::Result<Option<PathBuf>> {
    // Check if directory exists
    if !snapshot_dir.exists() {
        return Ok(None);
    }

    // Verify it's actually a directory. Safe to return error because we already
    // verified the file exists.
    if !snapshot_dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Provided path is not a directory"
        ));
    }

    // Read directory and find first matching file
    Ok(fs::read_dir(snapshot_dir)?
        .filter_map(|entry| entry.ok())
        .find(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("snapshot-")
        })
        .map(|entry| entry.path())
    )
}

pub (super) mod download {
    //! Provides utilities to download a snapshot.
    use {
        anyhow::{Result, anyhow},
        indicatif::{ProgressBar, ProgressStyle},
        log::*,
        reqwest::{self, IntoUrl},
        std::{
            fs,
            io::{BufWriter, Write},
            path::{Path, PathBuf},
        },
        tempfile,
    };

    /// Asynchronously streams a snapshot from the provided `url`
    /// to a file in the `snapshot_dir`, updating a CLI progress
    /// bar as it does.
    /// 
    /// The file only persists if the download is successful.
    /// 
    /// The snapshot_dir and all parent dirs will be automatically
    /// created if they do not already exist.
    pub (super) async fn download_snapshot(
        snapshot_dir: impl AsRef<Path>,
        reqwest_client: &reqwest::Client,
        url: impl IntoUrl
    ) -> Result<PathBuf> {
        fs::create_dir_all(&snapshot_dir)?;

        // First request to get the redirect URL
        let mut response = reqwest_client
            .get(url)
            .send()
            .await?;

        debug!("{:#?}", response);

        // Get the final URL (which contains the snapshot name) after potential redirects.
        let final_url = response.url().clone();
        let file_name = final_url
            .path_segments()
            .and_then(|segments| segments.last())
            .ok_or_else(|| anyhow!("Could not determine filename from URL"))?;
        info!("Filename: {}", file_name);

        // Final snapshot path.
        let file_path = snapshot_dir.as_ref().join(file_name);

        // Create a progress bar.
        let total_size = response.content_length().unwrap_or(0);
        let progress_bar = ProgressBar::new(total_size);
        progress_bar.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:50.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .unwrap()
            .progress_chars("#>-"));

        info!("Downloading file to: {}", file_path.to_string_lossy());

        // Download to a temp file first.
        let temp_file = tempfile::NamedTempFile::new_in(snapshot_dir.as_ref())?;

        // Create sub-scope to ensure writer is dropped before persisting file.
        {
            let mut writer = BufWriter::new(&temp_file);
            // Stream response in chunks.
            while let Some(chunk) = response.chunk().await? {
                writer.write_all(&chunk)?;
                progress_bar.inc(chunk.len() as u64);
            }
            writer.flush()?;
        }

        temp_file.persist(&file_path)?;
        progress_bar.finish_with_message("Download completed");
        info!("File downloaded successfully.");
        Ok(file_path)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[ignore = "Downloads a large file"]
        #[tokio::test]
        async fn test_download_snapshot() {
            let client = reqwest::Client::new();
            let url = "https://api.mainnet-beta.solana.com/incremental-snapshot.tar.bz2";
            let save_dir = tempfile::tempdir().unwrap();

            let snapshot_path = download_snapshot(save_dir.path(), &client, url)
                .await
                .unwrap();

            assert!(snapshot_path.is_file())
        }
    }
}