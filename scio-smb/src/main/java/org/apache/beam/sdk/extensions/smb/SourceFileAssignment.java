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
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.display.HasDisplayData;

/**
 * A file name assigner based on a specific output directory and file suffix. Optionally prepends
 * a timestamp to file names to ensure idempotence.
 */
public abstract class SourceFileAssignment implements Serializable, HasDisplayData {

  public abstract List<ResourceId> forBucket(BucketShardId id, BucketMetadata<?, ?> metadata);

  public abstract ResourceId forMetadata();

  public static DefaultSourceFileAssignment ofDefault(
      ResourceId filenamePrefix, String filenameSuffix, boolean doTimestampFiles
  ) {
    return new DefaultSourceFileAssignment(filenamePrefix, filenameSuffix, doTimestampFiles);
  }

  public static class DefaultSourceFileAssignment extends SourceFileAssignment {
    private final SinkFileAssignment underlying;

    DefaultSourceFileAssignment(
        ResourceId filenamePrefix, String filenameSuffix, boolean doTimestampFiles
    ) {
      this.underlying = new SinkFileAssignment(filenamePrefix, filenameSuffix, doTimestampFiles);
    }

    public List<ResourceId> forBucket(BucketShardId id, BucketMetadata<?, ?> metadata) {
      return Collections.singletonList(underlying.forBucket(id, metadata));
    }

    public ResourceId forMetadata() {
      return underlying.forMetadata();
    }

    @Override
    public void populateDisplayData(Builder builder) {
      underlying.populateDisplayData(builder);
    }
  }
}
