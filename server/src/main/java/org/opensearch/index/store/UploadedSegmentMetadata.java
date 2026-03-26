/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.util.Version;
import org.opensearch.common.annotation.PublicApi;

import java.nio.file.Path;

/**
 * Metadata of a segment that is uploaded to remote segment store.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.3.0")
public class UploadedSegmentMetadata {
    static final String SEPARATOR = "::";

    private final String originalFilename;
    private final String uploadedFilename;
    private final String checksum;
    private final String dataFormat;
    private final long length;
    private int writtenByMajor;

    public UploadedSegmentMetadata(String originalFilename, String uploadedFilename, String checksum, long length) {
        this.originalFilename = originalFilename;
        this.uploadedFilename = uploadedFilename;
        this.checksum = checksum;
        this.length = length;
        this.dataFormat = "lucene";
    }

    public UploadedSegmentMetadata(String originalFilename, String uploadedFilename, String checksum, long length, String dataFormat) {
        this.originalFilename = originalFilename;
        this.uploadedFilename = uploadedFilename;
        this.checksum = checksum;
        this.length = length;
        this.dataFormat = dataFormat;
    }

    @Override
    public String toString() {
        String base = String.join(SEPARATOR, originalFilename, uploadedFilename, checksum, String.valueOf(length), String.valueOf(writtenByMajor));
        if ("lucene".equals(dataFormat)) {
            return base;
        }
        return base + SEPARATOR + dataFormat;
    }

    public String getChecksum() {
        return this.checksum;
    }

    public long getLength() {
        return this.length;
    }

    public String getOriginalFilename() {
        return originalFilename;
    }

    public String getUploadedFilename() {
        return uploadedFilename;
    }

    public String getDataFormat() {
        return dataFormat;
    }

    public void setWrittenByMajor(int writtenByMajor) {
        if (writtenByMajor <= Version.LATEST.major && writtenByMajor >= Version.MIN_SUPPORTED_MAJOR) {
            this.writtenByMajor = writtenByMajor;
        } else {
            throw new IllegalArgumentException("Lucene major version supplied (" + writtenByMajor + ") is incorrect.");
        }
    }

    public static UploadedSegmentMetadata fromString(String uploadedFilename) {
        String filename = Path.of(uploadedFilename).getFileName().toString();
        String[] values = filename.split(SEPARATOR);

        // Extract dataFormat from position 5, default to "lucene" for backward compatibility
        String dataFormat = values.length >= 6 ? values[5] : "lucene";

        // Use correct 5-parameter constructor including dataFormat
        UploadedSegmentMetadata metadata = new UploadedSegmentMetadata(
            values[0],                  // originalFilename
            values[1],                  // uploadedFilename
            values[2],                  // checksum
            Long.parseLong(values[3]),  // length
            dataFormat                  // dataFormat
        );

        // Set writtenByMajor if present (only valid for lucene format)
        // ToDo: need to add check for non-optimized index here.
        metadata.setWrittenByMajor(Integer.parseInt(values[4]));

        return metadata;
    }
}
