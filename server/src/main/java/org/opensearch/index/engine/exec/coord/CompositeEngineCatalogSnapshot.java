/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.*;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.CatalogSnapshot;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.*;
import java.util.*;

@ExperimentalApi
public class CompositeEngineCatalogSnapshot extends CatalogSnapshot {

    private static final Logger logger = LogManager.getLogger(CompositeEngineCatalogSnapshot.class);

    public static final String CATALOG_SNAPSHOT_KEY = "_catalog_snapshot_";
    public static final String LAST_COMPOSITE_WRITER_GEN_KEY = "_last_composite_writer_gen_";
    private Map<String, String> userData;
    private List<Segment> segmentList;

    public CompositeEngineCatalogSnapshot(long id, long version, List<Segment> segmentList, Map<Long, CompositeEngineCatalogSnapshot> catalogSnapshotMap) {
        super("catalog_snapshot_" + id, id, version);
        this.segmentList = segmentList;
        this.userData = new HashMap<>();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public String serializeToString() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            this.writeTo(out);
            return Base64.getEncoder().encodeToString(out.bytes().toBytesRef().bytes);
        }
    }

    public List<Segment> getSegments() {
        return segmentList;
    }

    @Override
    public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Set<String> getDataFormats() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long getLastWriterGeneration() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Returns user data associated with this catalog snapshot.
     *
     * @return map of user data key-value pairs
     */
    public Map<String, String> getUserData() {
        return userData;
    }

    @Override
    protected void closeInternal() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setCatalogSnapshotMap(Map<Long, ? extends CatalogSnapshot> catalogSnapshotMap) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public  void setUserData(Map<String, String> userData, boolean b) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Object getReader(DataFormat dataFormat) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long getId() {
        return generation;
    }

    @Override
    public CompositeEngineCatalogSnapshot clone() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String toString() {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
