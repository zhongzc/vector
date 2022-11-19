use std::collections::btree_set::BTreeSet;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use std::{fs, io};

use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart, MultipartUpload, Part};
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures_util::StreamExt;
use md5::Digest;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio_util::time::DelayQueue;
use vector_common::internal_event::EventsSent;
use vector_core::event::Finalizable;
use vector_core::event::{Event, EventStatus};
use vector_core::sink::{StreamSink, VectorSink};

use crate::aws::{AwsAuthentication, RegionOrEndpoint};
use crate::{
    config::{
        AcknowledgementsConfig, DataType, GenerateConfig, Input, ProxyConfig, SinkConfig,
        SinkContext, SinkDescription,
    },
    sinks::{
        s3_common::{self, config::S3Options, service::S3Service},
        Healthcheck,
    },
    tls::TlsConfig,
};

// limit the chunk size to 8MB to avoid OOM
const S3_MULTIPART_UPLOAD_CHUNK_SIZE: usize = 8 * 1024 * 1024;
const S3_MULTIPART_UPLOAD_MAX_CHUNKS: usize = 10000;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct S3UploadFileConfig {
    pub bucket: String,
    #[serde(flatten)]
    pub options: S3Options,
    #[serde(flatten)]
    pub region: RegionOrEndpoint,
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub auth: AwsAuthentication,
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    /// The directory used to persist file checkpoint.
    ///
    /// By default, the global `data_dir` option is used. Please make sure the user Vector is running as has write permissions to this directory.
    pub data_dir: Option<PathBuf>,

    /// Delay between receiving upload event and beginning to upload file.
    #[serde(alias = "delay_upload", default = "default_delay_upload_secs")]
    pub delay_upload_secs: u64,

    /// The expire time of uploaded file records which used to prevent duplicate uploads.
    #[serde(alias = "expire_after", default = "default_expire_after_secs")]
    pub expire_after_secs: u64,
}

pub fn default_delay_upload_secs() -> u64 {
    10
}

pub fn default_expire_after_secs() -> u64 {
    1800
}

impl GenerateConfig for S3UploadFileConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            bucket: "".to_owned(),
            options: S3Options::default(),
            region: RegionOrEndpoint::default(),
            tls: None,
            auth: AwsAuthentication::default(),
            acknowledgements: Default::default(),

            data_dir: None,
            delay_upload_secs: default_delay_upload_secs(),
            expire_after_secs: default_expire_after_secs(),
        })
        .unwrap()
    }
}

inventory::submit! {
    SinkDescription::new::<S3UploadFileConfig>("aws_s3_upload_file")
}

#[async_trait::async_trait]
#[typetag::serde(name = "aws_s3_upload_file")]
impl SinkConfig for S3UploadFileConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let service = self.create_service(&cx.proxy).await?;
        let healthcheck = self.build_healthcheck(service.client())?;
        let sink = self.build_processor(service, cx)?;
        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Log)
    }

    fn sink_type(&self) -> &'static str {
        "aws_s3_upload_file"
    }

    fn acknowledgements(&self) -> Option<&AcknowledgementsConfig> {
        Some(&self.acknowledgements)
    }
}

impl S3UploadFileConfig {
    pub fn build_processor(
        &self,
        service: S3Service,
        cx: SinkContext,
    ) -> crate::Result<VectorSink> {
        let data_dir = cx
            .globals
            .resolve_and_make_data_subdir(self.data_dir.as_ref(), self.sink_type())?;
        let mut checkpointer = Checkpointer::new(data_dir);
        checkpointer.read_checkpoints();

        let sink = S3UploadFileSink::new(
            self.bucket.clone(),
            self.options.clone(),
            Duration::from_secs(self.delay_upload_secs),
            Duration::from_secs(self.expire_after_secs),
            service,
            checkpointer,
        );

        Ok(VectorSink::from_event_streamsink(sink))
    }

    pub fn build_healthcheck(&self, client: S3Client) -> crate::Result<Healthcheck> {
        s3_common::config::build_healthcheck(self.bucket.clone(), client)
    }

    pub async fn create_service(&self, proxy: &ProxyConfig) -> crate::Result<S3Service> {
        s3_common::config::create_service(&self.region, &self.auth, proxy, &self.tls).await
    }
}

struct S3UploadFileSink {
    pub service: S3Service,
    pub bucket: String,
    pub options: S3Options,
    pub delay_upload: Duration,
    pub expire_after: Duration,
    pub checkpointer: Checkpointer,
}

impl S3UploadFileSink {
    fn new(
        bucket: String,
        options: S3Options,
        delay_upload: Duration,
        expire_after: Duration,
        service: S3Service,
        checkpointer: Checkpointer,
    ) -> Self {
        Self {
            bucket,
            options,
            delay_upload,
            expire_after,
            service,
            checkpointer,
        }
    }

    fn event_as_key(event: &Event, bucket: &str) -> Option<UploadKey> {
        let log = event.maybe_as_log()?;
        let filename_val = log.get("message")?;
        let filename = String::from_utf8_lossy(filename_val.as_bytes()?);

        let object_key_val = log.get("key")?;
        let object_key = String::from_utf8_lossy(object_key_val.as_bytes()?);

        Some(UploadKey {
            bucket: bucket.to_owned(),
            object_key: object_key.to_string(),
            filename: filename.to_string(),
        })
    }

    async fn file_modified_time(filename: &str) -> io::Result<SystemTime> {
        tokio::fs::metadata(filename).await?.modified()
    }
}

struct UploadResponse {
    count: usize,
    events_byte_size: usize,
}

#[async_trait::async_trait]
impl StreamSink<Event> for S3UploadFileSink {
    async fn run(self: Box<Self>, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        let Self {
            service,
            bucket,
            options,
            delay_upload,
            expire_after,
            mut checkpointer,
        } = *self;

        let mut delay_queue = DelayQueue::new();
        let mut pending_uploads = HashSet::new();
        let mut uploader = S3Uploader::new(service.client(), options);

        loop {
            tokio::select! {
                event = input.next() => {
                    let mut event = if let Some(event) = event {
                        event
                    } else {
                        break;
                    };

                    let finalizers = event.take_finalizers();
                    if let Some(upload_key) = Self::event_as_key(&event, &bucket) {
                        let modified_time = match Self::file_modified_time(&upload_key.filename).await {
                            Ok(modified_time) => modified_time,
                            Err(err) => {
                                finalizers.update_status(EventStatus::Rejected);
                                error!(message = "Failed to get file modified time.", %err);
                                continue;
                            }
                        };

                        if !checkpointer.contains(&upload_key, modified_time) && !pending_uploads.contains(&upload_key) {
                            delay_queue.insert((upload_key.clone(), finalizers), delay_upload);
                            pending_uploads.insert(upload_key);
                        } else {
                            finalizers.update_status(EventStatus::Delivered);
                        }
                    } else {
                        finalizers.update_status(EventStatus::Rejected);
                    }
                }

                entry = delay_queue.next(), if !delay_queue.is_empty() => {
                    let (upload_key, finalizers) = if let Some(entry) = entry {
                        entry.into_inner()
                    } else {
                        // DelayQueue returns None if the queue is exhausted,
                        // however we disable the DelayQueue branch if there are
                        // no items in the queue.
                        unreachable!("an empty DelayQueue is never polled");
                    };
                    pending_uploads.remove(&upload_key);

                    let upload_time = SystemTime::now();
                    match uploader.upload(&upload_key).await {
                        Ok(response) => {
                            if response.count > 0 {
                                info!(
                                    message = "Uploaded file.",
                                    filename = %upload_key.filename,
                                    bucket = %upload_key.bucket,
                                    key = %upload_key.object_key,
                                    size = %response.events_byte_size,
                                );
                            }
                            finalizers.update_status(EventStatus::Delivered);
                            emit!(EventsSent {
                                count: response.count,
                                byte_size: response.events_byte_size,
                                output: None,
                            });
                            checkpointer.update(upload_key, upload_time, expire_after);
                        }
                        Err(error) => {
                            error!(
                                message = "Failed to upload file to S3.",
                                %error,
                                filename = %upload_key.filename,
                                bucket = %upload_key.bucket,
                                key = %upload_key.object_key,
                            );
                            finalizers.update_status(EventStatus::Rejected);
                        }
                    }
                    match checkpointer.write_checkpoints() {
                        Ok(count) => trace!(message = "Checkpoints written", %count),
                        Err(error) => error!(message = "Failed to write checkpoints.", %error),
                    }
                }
            }
        }

        Ok(())
    }
}

struct S3Uploader {
    client: S3Client,
    options: S3Options,
    etag_calculator: EtagCalculator,
}

impl S3Uploader {
    fn new(client: S3Client, options: S3Options) -> Self {
        Self {
            client,
            options,
            etag_calculator: EtagCalculator::default(),
        }
    }

    async fn upload(&mut self, upload_key: &UploadKey) -> io::Result<UploadResponse> {
        Ok(if self.need_upload(upload_key).await? {
            UploadResponse {
                count: 1,
                events_byte_size: self.do_upload(upload_key).await?,
            }
        } else {
            UploadResponse {
                count: 0,
                events_byte_size: 0,
            }
        })
    }

    async fn need_upload(&mut self, upload_key: &UploadKey) -> io::Result<bool> {
        if let Some(object_etag) = self.fetch_object_etag(upload_key).await {
            let etag = self.etag_calculator.file(&upload_key.filename).await?;
            if etag == object_etag {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn fetch_object_etag(&self, upload_key: &UploadKey) -> Option<String> {
        self.client
            .head_object()
            .bucket(&upload_key.bucket)
            .key(&upload_key.object_key)
            .send()
            .await
            .map(|res| res.e_tag)
            .ok()
            .flatten()
    }

    async fn do_upload(&mut self, upload_key: &UploadKey) -> io::Result<usize> {
        let mut file = File::open(&upload_key.filename).await?;

        let mut chunk = Vec::new();
        let n = (&mut file)
            .take(S3_MULTIPART_UPLOAD_CHUNK_SIZE as u64)
            .read_to_end(&mut chunk)
            .await?;
        if n < S3_MULTIPART_UPLOAD_CHUNK_SIZE {
            let uploader = self.multipart_uploader(upload_key, vec![], file);
            uploader.abort_all_uploads().await?;
            self.put_object(upload_key, chunk).await
        } else {
            let uploader = self.multipart_uploader(upload_key, chunk, file);
            Ok(uploader.upload().await?)
        }
    }

    async fn put_object(&self, upload_key: &UploadKey, body: Vec<u8>) -> io::Result<usize> {
        let content_md5 = EtagCalculator::content_md5(&body);
        let size = body.len();
        let tagging = self.options.tags.as_ref().map(|tags| {
            let mut tagging = url::form_urlencoded::Serializer::new(String::new());
            for (p, v) in tags {
                tagging.append_pair(&p, &v);
            }
            tagging.finish()
        });

        let _ = self
            .client
            .put_object()
            .body(ByteStream::from(body))
            .bucket(&upload_key.bucket)
            .key(&upload_key.object_key)
            .set_content_encoding(self.options.content_encoding.clone())
            .set_content_type(self.options.content_type.clone())
            .set_acl(self.options.acl.map(Into::into))
            .set_grant_full_control(self.options.grant_full_control.clone())
            .set_grant_read(self.options.grant_read.clone())
            .set_grant_read_acp(self.options.grant_read_acp.clone())
            .set_grant_write_acp(self.options.grant_write_acp.clone())
            .set_server_side_encryption(self.options.server_side_encryption.map(Into::into))
            .set_ssekms_key_id(self.options.ssekms_key_id.clone())
            .set_storage_class(self.options.storage_class.map(Into::into))
            .set_tagging(tagging)
            .content_md5(content_md5)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(size)
    }

    fn multipart_uploader<'a, 'b>(
        &'a mut self,
        upload_key: &'b UploadKey,
        chunk: Vec<u8>,
        file: File,
    ) -> MultipartUploader<'a, 'b> {
        MultipartUploader {
            client: &self.client,
            options: &self.options,
            upload_key,

            upload_id: "".to_owned(),
            file,
            chunk,
            part_number: 1,
            completed_parts: vec![],
        }
    }
}

struct MultipartUploader<'a, 'b> {
    client: &'a S3Client,
    options: &'a S3Options,
    upload_key: &'b UploadKey,

    upload_id: String,
    file: File,
    chunk: Vec<u8>,
    part_number: i32,
    completed_parts: Vec<CompletedPart>,
}

impl<'a, 'b> MultipartUploader<'a, 'b> {
    async fn upload(mut self) -> io::Result<usize> {
        self.initiate_upload().await?;

        let mut uploaded_size = 0;
        while !self.chunk.is_empty() {
            if self.part_number as usize > S3_MULTIPART_UPLOAD_MAX_CHUNKS {
                return Err(io::Error::new(io::ErrorKind::Other, "file is too large"));
            }

            let n = self.upload_part().await?;
            uploaded_size += n;

            self.chunk.clear();
            self.chunk.reserve(S3_MULTIPART_UPLOAD_CHUNK_SIZE);
            (&mut self.file)
                .take(S3_MULTIPART_UPLOAD_CHUNK_SIZE as u64)
                .read_to_end(&mut self.chunk)
                .await?;
            self.part_number += 1;
        }

        self.complete_upload().await?;
        Ok(uploaded_size)
    }

    async fn initiate_upload(&mut self) -> io::Result<()> {
        let uploads = self.list_existing_uploads().await?;
        if uploads.is_empty() {
            self.upload_id = self.create_upload().await?;
            return Ok(());
        }

        // only recover the latest multipart upload, abort others
        let upload_id = self.cleanup_uploads_except_latest(uploads).await?;
        let parts = self.list_parts(&upload_id).await?;
        if parts.is_empty() {
            self.upload_id = upload_id;
            return Ok(());
        }

        if self.verify_and_advance(parts, &upload_id).await? {
            self.upload_id = upload_id;
            return Ok(());
        }

        self.abort_upload(upload_id).await?;
        self.upload_id = self.create_upload().await?;
        // `verify_and_advance` modified these fields, reset them
        self.file = File::open(&self.upload_key.filename).await?;
        self.chunk.clear();
        (&mut self.file)
            .take(S3_MULTIPART_UPLOAD_CHUNK_SIZE as u64)
            .read_to_end(&mut self.chunk)
            .await?;
        self.part_number = 1;
        self.completed_parts.clear();
        Ok(())
    }

    async fn abort_all_uploads(&self) -> io::Result<()> {
        let uploads = self.list_existing_uploads().await?;
        for upload in uploads {
            let upload_id = upload.upload_id.unwrap_or_default();
            info!(
                message = "Cleaned up unused multipart upload",
                filename = %self.upload_key.filename,
                bucket = %self.upload_key.bucket,
                key = %self.upload_key.object_key,
                %upload_id,
            );
            self.abort_upload(upload_id).await?;
        }
        Ok(())
    }

    async fn list_existing_uploads(&self) -> io::Result<Vec<MultipartUpload>> {
        let uploads = self
            .client
            .list_multipart_uploads()
            .bucket(&self.upload_key.bucket)
            .prefix(&self.upload_key.object_key)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(uploads.uploads.unwrap_or_default())
    }

    async fn create_upload(&mut self) -> io::Result<String> {
        let tagging = self.options.tags.as_ref().map(|tags| {
            let mut tagging = url::form_urlencoded::Serializer::new(String::new());
            for (p, v) in tags {
                tagging.append_pair(&p, &v);
            }
            tagging.finish()
        });

        let response = self
            .client
            .create_multipart_upload()
            .bucket(&self.upload_key.bucket)
            .key(&self.upload_key.object_key)
            .set_content_encoding(self.options.content_encoding.clone())
            .set_content_type(self.options.content_type.clone())
            .set_acl(self.options.acl.map(Into::into))
            .set_grant_full_control(self.options.grant_full_control.clone())
            .set_grant_read(self.options.grant_read.clone())
            .set_grant_read_acp(self.options.grant_read_acp.clone())
            .set_grant_write_acp(self.options.grant_write_acp.clone())
            .set_server_side_encryption(self.options.server_side_encryption.map(Into::into))
            .set_ssekms_key_id(self.options.ssekms_key_id.clone())
            .set_storage_class(self.options.storage_class.map(Into::into))
            .set_tagging(tagging)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(response.upload_id.unwrap_or_default())
    }

    async fn cleanup_uploads_except_latest(
        &self,
        mut uploads: Vec<MultipartUpload>,
    ) -> io::Result<String> {
        uploads.sort_unstable_by_key(|a| {
            a.initiated
                .as_ref()
                .map(|a| a.as_nanos())
                .unwrap_or_default()
        });
        let upload = uploads.pop().unwrap();

        // abort older uploads
        for upload in uploads {
            let upload_id = upload.upload_id.unwrap_or_default();
            info!(
                message = "Cleaned up unused multipart upload",
                filename = %self.upload_key.filename,
                bucket = %self.upload_key.bucket,
                key = %self.upload_key.object_key,
                %upload_id,
            );
            self.abort_upload(upload_id).await?;
        }

        Ok(upload.upload_id.unwrap_or_default())
    }

    async fn abort_upload(&self, upload_id: String) -> io::Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.upload_key.bucket)
            .key(&self.upload_key.object_key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(())
    }

    async fn verify_and_advance(
        &mut self,
        mut parts: Vec<Part>,
        upload_id: &str,
    ) -> io::Result<bool> {
        let mut recovered_part_size = 0;
        parts.sort_unstable_by_key(|a| a.part_number);
        for part in parts {
            // check part number
            let part_number = part.part_number;
            let expected_part_number = self.part_number;
            if part_number != expected_part_number {
                warn!(
                    message = "Unexpected part number, aborted multipart upload.",
                    filename = %self.upload_key.filename,
                    bucket = %self.upload_key.bucket,
                    key = %self.upload_key.object_key,
                    %part_number,
                    %expected_part_number,
                    %upload_id,
                );
                return Ok(false);
            }

            // check etag
            let expected_part_etag = EtagCalculator::part(&self.chunk);
            let part_etag = part.e_tag.unwrap_or_default();
            if part_etag != expected_part_etag {
                warn!(
                    message = "Unexpected part etag, aborted multipart upload.",
                    filename = %self.upload_key.filename,
                    bucket = %self.upload_key.bucket,
                    key = %self.upload_key.object_key,
                    %part_etag,
                    %expected_part_etag,
                    %upload_id,
                );
                return Ok(false);
            }

            let completed_part = CompletedPart::builder()
                .e_tag(part_etag)
                .part_number(part_number)
                .build();
            self.completed_parts.push(completed_part);
            recovered_part_size += part.size;

            self.chunk.clear();
            (&mut self.file)
                .take(S3_MULTIPART_UPLOAD_CHUNK_SIZE as u64)
                .read_to_end(&mut self.chunk)
                .await?;
            self.part_number += 1;
        }

        info!(
            message = "Resumed upload",
            filename = %self.upload_key.filename,
            bucket = %self.upload_key.bucket,
            key = %self.upload_key.object_key,
            %recovered_part_size,
            %upload_id,
        );
        Ok(true)
    }

    async fn list_parts(&self, upload_id: &str) -> io::Result<Vec<Part>> {
        let res = self
            .client
            .list_parts()
            .bucket(&self.upload_key.bucket)
            .key(&self.upload_key.object_key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(res.parts.unwrap_or_default())
    }

    async fn upload_part(&mut self) -> io::Result<usize> {
        let body = std::mem::take(&mut self.chunk);
        let size = body.len();
        let content_md5 = EtagCalculator::content_md5(&body);
        let response = self
            .client
            .upload_part()
            .body(ByteStream::from(body))
            .bucket(&self.upload_key.bucket)
            .key(&self.upload_key.object_key)
            .part_number(self.part_number)
            .upload_id(&self.upload_id)
            .content_md5(content_md5)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let completed_part = CompletedPart::builder()
            .part_number(self.part_number)
            .e_tag(response.e_tag.unwrap_or_default())
            .build();
        self.completed_parts.push(completed_part);

        Ok(size)
    }

    async fn complete_upload(&mut self) -> io::Result<()> {
        let completed_parts = std::mem::take(&mut self.completed_parts);
        let completed_multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();
        let _ = self
            .client
            .complete_multipart_upload()
            .bucket(&self.upload_key.bucket)
            .key(&self.upload_key.object_key)
            .upload_id(&self.upload_id)
            .multipart_upload(completed_multipart_upload)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(())
    }
}

#[derive(Default)]
struct EtagCalculator {
    chunk: Vec<u8>,
    concat_md5: Vec<u8>,
}

impl EtagCalculator {
    fn part(chunk: &[u8]) -> String {
        format!("\"{:x}\"", md5::Md5::digest(chunk))
    }

    fn content_md5(chunk: &[u8]) -> String {
        base64::encode(md5::Md5::digest(chunk))
    }

    async fn file(&mut self, filename: impl AsRef<Path>) -> io::Result<String> {
        let mut chunk_count = 0;
        let mut file = File::open(filename).await?;
        let mut total_size = 0;
        loop {
            self.chunk.clear();
            let read_size = (&mut file)
                .take(S3_MULTIPART_UPLOAD_CHUNK_SIZE as u64)
                .read_to_end(&mut self.chunk)
                .await?;
            total_size += read_size;
            if read_size == 0 {
                break;
            }
            chunk_count += 1;
            let digest: [u8; 16] = md5::Md5::digest(&self.chunk).into();
            self.concat_md5.extend_from_slice(&digest);
            if read_size < S3_MULTIPART_UPLOAD_CHUNK_SIZE {
                break;
            }
            if chunk_count > S3_MULTIPART_UPLOAD_MAX_CHUNKS {
                return Err(io::Error::new(io::ErrorKind::Other, "file is too large"));
            }
        }

        if self.concat_md5.is_empty() {
            let digest: [u8; 16] = md5::Md5::digest(&[]).into();
            self.concat_md5.extend_from_slice(&digest);
        }

        let res = if total_size >= S3_MULTIPART_UPLOAD_CHUNK_SIZE {
            format!(
                "\"{:x}-{}\"",
                md5::Md5::digest(&self.concat_md5),
                chunk_count
            )
        } else {
            format!("\"{}\"", hex::encode(&self.concat_md5))
        };

        // limit the capacity to avoid occupying too much memory
        const MAX_CAPACITY: usize = 10 * 1024; // 10KiB
        self.concat_md5.clear();
        self.chunk.clear();
        self.concat_md5.shrink_to(MAX_CAPACITY);
        self.chunk.shrink_to(MAX_CAPACITY);

        Ok(res)
    }
}

const TMP_FILE_NAME: &str = "checkpoints.new.json";
pub const CHECKPOINT_FILE_NAME: &str = "checkpoints.json";

struct Checkpointer {
    tmp_file_path: PathBuf,
    stable_file_path: PathBuf,
    checkpoints: CheckPointsView,
    last: State,
}

impl Checkpointer {
    pub fn new(data_dir: PathBuf) -> Checkpointer {
        let tmp_file_path = data_dir.join(TMP_FILE_NAME);
        let stable_file_path = data_dir.join(CHECKPOINT_FILE_NAME);
        Checkpointer {
            tmp_file_path,
            stable_file_path,
            checkpoints: CheckPointsView::default(),
            last: State::V1 {
                checkpoints: BTreeSet::default(),
            },
        }
    }

    pub fn contains(&self, key: &UploadKey, upload_time_after: SystemTime) -> bool {
        self.checkpoints.contains(key, upload_time_after)
    }

    pub fn update(&mut self, key: UploadKey, upload_time: SystemTime, expire_after: Duration) {
        self.checkpoints
            .update(key, upload_time.into(), (upload_time + expire_after).into());
    }

    /// Read persisted checkpoints from disk, preferring the new JSON file format.
    pub fn read_checkpoints(&mut self) {
        // First try reading from the tmp file location. If this works, it means
        // that the previous process was interrupted in the process of
        // checkpointing and the tmp file should contain more recent data that
        // should be preferred.
        match self.read_checkpoints_file(&self.tmp_file_path) {
            Ok(state) => {
                warn!(message = "Recovered checkpoint data from interrupted process.");
                self.checkpoints.set_state(&state);
                self.last = state;

                // Try to move this tmp file to the stable location so we don't
                // immediately overwrite it when we next persist checkpoints.
                if let Err(error) = fs::rename(&self.tmp_file_path, &self.stable_file_path) {
                    warn!(message = "Error persisting recovered checkpoint file.", %error);
                }
                return;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // This is expected, so no warning needed
            }
            Err(error) => {
                error!(message = "Unable to recover checkpoint data from interrupted process.", %error);
            }
        }

        // Next, attempt to read checkpoints from the stable file location. This
        // is the expected location, so warn more aggressively if something goes
        // wrong.
        match self.read_checkpoints_file(&self.stable_file_path) {
            Ok(state) => {
                info!(message = "Loaded checkpoint data.");
                self.checkpoints.set_state(&state);
                self.last = state;
                return;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // This is expected, so no warning needed
            }
            Err(error) => {
                warn!(message = "Unable to load checkpoint data.", %error);
                return;
            }
        }
    }

    /// Persist the current checkpoints state to disk, making our best effort to
    /// do so in an atomic way that allow for recovering the previous state in
    /// the event of a crash.
    pub fn write_checkpoints(&mut self) -> Result<usize, io::Error> {
        self.checkpoints.remove_expired();
        let state = self.checkpoints.get_state();

        if self.last == state {
            return Ok(self.checkpoints.len());
        }

        // Write the new checkpoints to a tmp file and flush it fully to
        // disk. If vector dies anywhere during this section, the existing
        // stable file will still be in its current valid state and we'll be
        // able to recover.
        let mut f = io::BufWriter::new(fs::File::create(&self.tmp_file_path)?);
        serde_json::to_writer(&mut f, &state)?;
        f.into_inner()?.sync_all()?;

        // Once the temp file is fully flushed, rename the tmp file to replace
        // the previous stable file. This is an atomic operation on POSIX
        // systems (and the stdlib claims to provide equivalent behavior on
        // Windows), which should prevent scenarios where we don't have at least
        // one full valid file to recover from.
        fs::rename(&self.tmp_file_path, &self.stable_file_path)?;

        self.last = state;
        Ok(self.checkpoints.len())
    }

    fn read_checkpoints_file(&self, path: &Path) -> Result<State, io::Error> {
        let reader = io::BufReader::new(fs::File::open(path)?);
        serde_json::from_reader(reader).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
struct UploadKey {
    filename: String,
    bucket: String,
    object_key: String,
}

#[derive(Default)]
struct CheckPointsView {
    upload_times: HashMap<UploadKey, DateTime<Utc>>,
    expire_times: HashMap<UploadKey, DateTime<Utc>>,
}

impl CheckPointsView {
    pub fn get_state(&self) -> State {
        State::V1 {
            checkpoints: self
                .expire_times
                .iter()
                .map(|(key, time)| Checkpoint {
                    upload_key: key.clone(),
                    expire_at: *time,
                    upload_at: self.upload_times.get(key).copied().unwrap_or_else(Utc::now),
                })
                .collect(),
        }
    }

    pub fn set_state(&mut self, state: &State) {
        match state {
            State::V1 { checkpoints } => {
                for checkpoint in checkpoints {
                    self.expire_times
                        .insert(checkpoint.upload_key.clone(), checkpoint.expire_at);
                    self.upload_times
                        .insert(checkpoint.upload_key.clone(), checkpoint.upload_at);
                }
            }
        }
    }

    pub fn contains(&self, key: &UploadKey, upload_time_after: SystemTime) -> bool {
        let upload_time_after = DateTime::<Utc>::from(upload_time_after);
        self.upload_times
            .get(key)
            .map(|time| time >= &upload_time_after)
            .unwrap_or_default()
    }

    pub fn update(&mut self, key: UploadKey, upload_at: DateTime<Utc>, expire_at: DateTime<Utc>) {
        self.upload_times.insert(key.clone(), upload_at);
        self.expire_times.insert(key.clone(), expire_at);
    }

    pub fn remove_expired(&mut self) {
        let now = Utc::now();
        let mut expired = Vec::new();
        for (key, expire_time) in &self.expire_times {
            if expire_time < &now {
                expired.push(key.clone());
            }
        }
        for key in expired {
            self.upload_times.remove(&key);
            self.expire_times.remove(&key);
        }
    }

    pub fn len(&self) -> usize {
        self.upload_times.len()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "version", rename_all = "snake_case")]
enum State {
    #[serde(rename = "1")]
    V1 { checkpoints: BTreeSet<Checkpoint> },
}

/// A simple JSON-friendly struct of the fingerprint/position pair, since
/// fingerprints as objects cannot be keys in a plain JSON map.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
struct Checkpoint {
    upload_key: UploadKey,
    upload_at: DateTime<Utc>,
    expire_at: DateTime<Utc>,
}
