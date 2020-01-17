/*
 * Copyright 2019 Spotify AB.
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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Naming policy for SMB files, similar to {@link
 * org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy}.
 *
 * <p>File names are assigned uniquely per {@link BucketShardId}. This class functions differently
 * for the initial write to temp files, and the move of those files to their final destination. This
 * is because temp writes need to be idempotent in case of bundle failure, and are thus timestamped
 * to ensure an uncorrupted write result when a bundle succeeds.
 */
public final class SMBFilenamePolicy implements Serializable {

  private static final String TEMP_DIRECTORY_PREFIX = ".temp-beam";
  private static final AtomicLong TEMP_COUNT = new AtomicLong(0);
  private static final DateTimeFormatter TEMPDIR_TIMESTAMP =
      DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm-ss");

  private final String timestamp = Instant.now().toString(TEMPDIR_TIMESTAMP);
  private final Long tempId = TEMP_COUNT.getAndIncrement();

  private final ResourceId directory;
  private final String filenameSuffix;

  private SinkFileAssignment sinkAssignment;
  private SourceFileAssignment sourceAssignment;

  public SMBFilenamePolicy(ResourceId directory, String filenameSuffix) {
    Preconditions.checkArgument(directory.isDirectory(), "ResourceId must be a directory");
    this.directory = directory;
    this.filenameSuffix = filenameSuffix;
  }

  public SMBFilenamePolicy withSinkAssignment(SinkFileAssignment fileAssignment) {
    this.sinkAssignment = fileAssignment;
    return this;
  }

  public SMBFilenamePolicy withSourceAssignment(SourceFileAssignment fileAssignment) {
    this.sourceAssignment = fileAssignment;
    return this;
  }

  public SinkFileAssignment forSink() {
    if (this.sinkAssignment == null) {
      this.sinkAssignment = new SinkFileAssignment(directory, filenameSuffix, false);
    }

    return this.sinkAssignment;
  }

  public SourceFileAssignment forSource() {
    if (this.sourceAssignment == null) {
      this.sourceAssignment = SourceFileAssignment.ofDefault(directory, filenameSuffix, false);
    }

    return this.sourceAssignment;
  }

  SinkFileAssignment forTempFiles(ResourceId tempDirectory) {
    final String tempDirName =
        String.format(TEMP_DIRECTORY_PREFIX + "-%s-%08d", timestamp, getTempId());
    return new SinkFileAssignment(
        tempDirectory
            .getCurrentDirectory()
            .resolve(tempDirName, StandardResolveOptions.RESOLVE_DIRECTORY),
        filenameSuffix,
        true);
  }

  @VisibleForTesting
  Long getTempId() {
    return tempId;
  }
}
