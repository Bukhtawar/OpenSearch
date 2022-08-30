/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.action.ActionListener;
import org.opensearch.index.translog.FileSnapshot;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

public interface TransferService {

    void uploadFileAsync(final FileSnapshot fileSnapshot, Iterable<String> remotePath, ActionListener<FileSnapshot> listener);

    void uploadFile(final FileSnapshot fileSnapshot, Iterable<String> remotePath) throws IOException;

    Collection<String> listFilesByPrefix(String prefix, Iterable<String> remotePath) throws IOException;

    InputStream readFile(String filename, Iterable<String> remotePath) throws IOException;
}
