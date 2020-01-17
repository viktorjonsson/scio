/*
 * Copyright 2020 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.extensions.smb;

import java.io.Serializable;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * A file name assigner based on a specific output directory and file suffix. Optionally prepends
 * a timestamp to file names to ensure idempotence.
 */
public final class SinkFileAssignment implements Serializable, HasDisplayData {

  private static final String NULL_KEYS_BUCKET_TEMPLATE = "null-keys";
  private static final String NUMERIC_BUCKET_TEMPLATE = "%05d-of-%05d";
  private static final String BUCKET_ONLY_TEMPLATE = "bucket-%s%s";
  private static final String BUCKET_SHARD_TEMPLATE = "bucket-%s-shard-%05d-of-%05d%s";
  private static final String METADATA_FILENAME = "metadata.json";
  private static final DateTimeFormatter TEMPFILE_TIMESTAMP =
      DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm-ss-");

  private final ResourceId filenamePrefix;
  private final String filenameSuffix;
  private final boolean doTimestampFiles;

  SinkFileAssignment(ResourceId filenamePrefix, String filenameSuffix, boolean doTimestampFiles) {
    this.filenamePrefix = filenamePrefix;
    this.filenameSuffix = filenameSuffix;
    this.doTimestampFiles = doTimestampFiles;
  }

  public ResourceId forBucket(BucketShardId id, BucketMetadata<?, ?> metadata) {
    Preconditions.checkArgument(
        id.getBucketId() < metadata.getNumBuckets(),
        "Can't assign a filename for bucketShardId %s: max number of buckets is %s",
        id,
        metadata.getNumBuckets());

    Preconditions.checkArgument(
        id.getShardId() < metadata.getNumShards(),
        "Can't assign a filename for bucketShardId %s: max number of shards is %s",
        id,
        metadata.getNumBuckets());

    final String bucketName =
        id.isNullKeyBucket()
            ? NULL_KEYS_BUCKET_TEMPLATE
            : String.format(NUMERIC_BUCKET_TEMPLATE, id.getBucketId(), metadata.getNumBuckets());

    final String timestamp = doTimestampFiles ? Instant.now().toString(TEMPFILE_TIMESTAMP) : "";
    String filename = metadata.getNumShards() == 1 ?
        String.format(BUCKET_ONLY_TEMPLATE, bucketName, filenameSuffix) :
        String.format(
            BUCKET_SHARD_TEMPLATE,
            bucketName,
            id.getShardId(),
            metadata.getNumShards(),
            filenameSuffix);

    return filenamePrefix.resolve(timestamp + filename, StandardResolveOptions.RESOLVE_FILE);
  }

  public ResourceId forMetadata() {
    String timestamp = doTimestampFiles ? Instant.now().toString(TEMPFILE_TIMESTAMP) : "";
    return filenamePrefix.resolve(
        timestamp + METADATA_FILENAME, StandardResolveOptions.RESOLVE_FILE);
  }

  @Override
  public void populateDisplayData(Builder builder) {
    builder.add(DisplayData.item("directory", filenamePrefix.toString()));
    builder.add(DisplayData.item("filenameSuffix", filenameSuffix));
  }
}
