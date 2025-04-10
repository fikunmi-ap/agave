//! Provides utilities to download, decompress and unpack a snapshot.

pub mod download {
    //! Provides utilities to download a snapshot.
    use {
        indicatif::ProgressBar,
        reqwest::StatusCode,
        std::{
            fs::File,
            io::{BufWriter, Write},
            path::{Path, PathBuf},
        },
        thiserror,
    };

    #[derive(thiserror::Error, Debug)]
    pub enum DownloadError {
        #[error("HTTP request failed: {status_code}")]
        RequestFailed { status_code: StatusCode },

        #[error(transparent)]
        Reqwest(#[from] reqwest::Error),

        #[error("IO Error")]
        Io(#[from] std::io::Error),
    }

    /// Asynchronously downloads a file from the `url` and saves it to
    /// `save_path`.
    ///
    /// It initially saves to a temporary file and only renames once the
    /// download is complete.
    pub async fn download_snapshot(
        client: &reqwest::Client,
        url: &str,
        save_path: &Path,
        progress_bar: &ProgressBar,
    ) -> Result<PathBuf, DownloadError> {
        let mut response = client.get(url).send().await?;

        if !response.status().is_success() {
            return Err(DownloadError::RequestFailed {
                status_code: response.status(),
            });
        }

        let save_file_path = compute_save_file_path(save_path, &response)?;
        if let Some(save_dir) = save_file_path.parent() {
            std::fs::create_dir_all(save_dir)?;
        }

        let file_length = response.content_length().unwrap_or(0); // Might not be provided.

        progress_bar.set_length(file_length);

        // Use temporary file for safety.
        let tmp_path = format!("{}.download", save_file_path.to_string_lossy());
        let file = File::create(&tmp_path)?;

        // To ensure writer gets dropped before trying to rename the file.
        {
            let mut writer = BufWriter::new(file);

            while let Some(chunk) = response.chunk().await? {
                writer.write_all(&chunk)?;
                progress_bar.inc(chunk.len() as u64);
            }

            writer.flush()?;
        }

        std::fs::rename(&tmp_path, &save_file_path)?;

        progress_bar.finish_with_message(format!(
            "Download complete. File saved to: {}",
            save_file_path.to_string_lossy()
        ));

        Ok(save_file_path)
    }

    /// Computes the full file path for the downloaded file.
    /// - If a file name is provided (heuristically determined by checking if the path ends with an extenstion)
    /// in the save path, then use that.
    /// - If no file name is provided in the save path then we get it from the URL.
    fn compute_save_file_path(
        save_path: &Path,
        response: &reqwest::Response,
    ) -> Result<PathBuf, DownloadError> {
        if save_path.extension().is_some() {
            return Ok(save_path.to_path_buf());
        }

        // Get URL from the response because of the possibility of redirects.
        let url = response.url();
        let file_name = get_file_name_from_url(url);
        Ok(save_path.join(file_name))
    }

    fn get_file_name_from_url(url: &reqwest::Url) -> String {
        let file_name = url.path_segments().unwrap().last().unwrap(); // type safety allows unwraping.
        file_name.to_string()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_get_file_name_from_url() {
            let url = "https://mainnet-beta-rpc.solana.com/fake_snapshot";
            let parsed_url = reqwest::Url::parse(url).unwrap();
            let computed_file_name = get_file_name_from_url(&parsed_url);
            assert_eq!(computed_file_name, "fake_snapshot");
        }

        #[ignore = "Downloads a large file"]
        #[tokio::test]
        async fn test_download_file() {
            let client = reqwest::Client::new();
            let url = "https://api.mainnet-beta.solana.com/incremental-snapshot.tar.bz2";
            let save_dir = tempfile::tempdir().unwrap();
            let progress_bar = ProgressBar::new(0);

            download_snapshot(&client, url, save_dir.path(), &progress_bar)
                .await
                .unwrap();
        }
    }
}


pub mod decompress {
    //! Provides utilities to decompress and unpack a snapshot.
    use {
        bzip2::read::BzDecoder, indicatif::ProgressBar, 
        std::{
            fs,
            io::{BufReader, Read, Write},
            path::Path,
        },
        tar::Archive, 
        tempfile::NamedTempFile, 
        zstd::stream::read::Decoder as ZstdDecoder,
    };

    #[derive(thiserror::Error, Debug)]
    pub enum DecompressionError {
        #[error(transparent)]
        Io(#[from] std::io::Error),

        #[error("Unsupported archive type. Only bz2 and zst are supported")]
        UnsupportedArchiveType,

        #[error("Could not determine archive type")]
        UnknownArchiveType,
    }

    /// Decompresses and unpacks a snapshot.
    pub fn decompress_and_unpack_snapshot(
        // TODO: Current approach decompresses to a temporary file before
        // unpacking. Can stream from decompression to unpacking through 
        // buffer instead.
        compressed_file_path: &Path,
        decompressed_file_path: &Path,
    ) -> Result<(), DecompressionError> {
        let tmp_tar_file = NamedTempFile::new()?;
        let decompression_progress_bar =  ProgressBar::new_spinner();
        match compressed_file_path.extension() {
            Some(extension) => match extension.to_string_lossy().as_ref() {
                "bz2" => decompress_snapshot_bz2(
                    compressed_file_path,
                    tmp_tar_file.path(),
                    &decompression_progress_bar
                )?,
                "zst" => decompress_snapshot_zstd(
                    compressed_file_path,
                    tmp_tar_file.path(),
                    &decompression_progress_bar
                )?,
                _ => return Err(DecompressionError::UnsupportedArchiveType)
            },

            None => return Err(DecompressionError::UnknownArchiveType),
        }
        decompression_progress_bar.finish_with_message("Decompression complete");

        eprintln!("Unpacking archive...");
        let unpacking_progress_bar = ProgressBar::new(0);
        if !decompressed_file_path.exists(){
            std::fs::create_dir_all(decompressed_file_path)?;
        }

        unpack_snapshot(tmp_tar_file.path(),
            decompressed_file_path,
            &unpacking_progress_bar
        )?;
        unpacking_progress_bar.finish_with_message("Unpacking complete");
        eprintln!("Unpacking complete!");
        eprintln!("Snapshot successfully decompressed and unpacked!");
        eprintln!("File saved to: {}", decompressed_file_path.to_string_lossy());

        Ok(())
    }

    fn unpack_snapshot(
        tar_file_path: &Path,
        unpacked_file_dir: &Path,
        progress_bar: &ProgressBar
    ) -> Result<(), DecompressionError> {
        let tar_file = fs::File::open(tar_file_path)?;
        let file_size = tar_file.metadata()?.len();
        progress_bar.set_length(file_size);

        let mut archive = Archive::new(
            BufReader::new(tar_file)
        );

        for entry in archive.entries()? {
            let mut entry = entry?;
            let entry_size = entry.size();
            entry.unpack_in(unpacked_file_dir)?;
            progress_bar.inc(entry_size);
        }
        Ok(())
    }

    fn decompress_snapshot_bz2(
        compressed_file_path: &Path,
        decompressed_file_path: &Path,
        progress_bar: &ProgressBar,
    ) -> Result<(), DecompressionError> {
        let compressed_file = fs::File::open(compressed_file_path)?;
        let file_size = compressed_file.metadata()?.len();
        progress_bar.set_length(file_size); // N.B: decompressed file is larger.

        let mut decompressed_file = fs::File::create(decompressed_file_path)?;
        
        let buf_reader = BufReader::new(compressed_file);
        let buf_reader_capacity = buf_reader.capacity();
        let mut decoder = BzDecoder::new(buf_reader);
        let mut buffer = vec![0; buf_reader_capacity];

        loop {
            let bytes_read = decoder.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            decompressed_file.write_all(&buffer[..bytes_read])?;
            progress_bar.inc(bytes_read as u64)
        }
        progress_bar.finish_with_message("Decompression complete");

        Ok(())
    }

    fn decompress_snapshot_zstd(
        compressed_file_path: &Path,
        decompressed_file_path: &Path,
        progress_bar: &ProgressBar,
    ) -> Result<(), DecompressionError> {
        let compressed_file = fs::File::open(compressed_file_path)?;
        let file_size = compressed_file.metadata()?.len();
        progress_bar.set_length(file_size); // N.B: decompressed file is larger.

        let mut decompressed_file = fs::File::create(decompressed_file_path)?;

        let buf_reader = BufReader::new(compressed_file);
        let buf_reader_capacity = buf_reader.capacity();
        let mut decoder = ZstdDecoder::new(
            buf_reader
        )?;
        let mut buffer = vec![0; buf_reader_capacity];

        loop {
            let bytes_read = decoder.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            decompressed_file.write_all(&buffer[..bytes_read])?;
            progress_bar.inc(bytes_read as u64);
        }
        progress_bar.finish_with_message("Decompression complete");

        Ok(())
    }
}