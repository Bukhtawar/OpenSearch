/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.checksum;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;

/**
 * Checksum handler for Lucene segment files.
 *
 * <p>Reads the checksum from the Lucene codec footer — an O(1) operation
 * since it only reads the last 16 bytes of the file.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class LuceneChecksumHandler implements ChecksumHandler {

    private static final String FORMAT_NAME = "lucene";

    @Override
    public String getFormatName() {
        return FORMAT_NAME;
    }

    @Override
    public long calculateChecksum(IndexInput input) throws IOException {
        return CodecUtil.retrieveChecksum(input);
    }
}
