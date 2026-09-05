/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.update;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link UpdateHelper#coveringFieldPaths}: the flattening of an update document into
 * the leaf paths whose values replace the stored subtree wholesale under
 * {@code XContentHelper.update} merge semantics (objects merge recursively; scalars, nulls, and
 * arrays replace).
 */
public class UpdateHelperCoveringPathsTests extends OpenSearchTestCase {

    public void testScalarsNullsAndArraysAreCoveringLeaves() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("title", "t");
        doc.put("count", 3);
        doc.put("deleted", null);
        doc.put("tags", Arrays.asList("a", "b"));

        assertEquals(Set.of("title", "count", "deleted", "tags"), UpdateHelper.coveringFieldPaths(doc));
    }

    public void testObjectsRecurseToDottedLeaves() {
        Map<String, Object> address = new LinkedHashMap<>();
        address.put("city", "x");
        Map<String, Object> user = new LinkedHashMap<>();
        user.put("name", "n");
        user.put("address", address);
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("user", user);

        // user.name and user.address.city are covering; "user" itself is NOT (objects merge).
        assertEquals(Set.of("user.name", "user.address.city"), UpdateHelper.coveringFieldPaths(doc));
    }

    public void testListOfObjectsIsASingleCoveringLeaf() {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("x", 1);
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("events", Arrays.asList(event));

        // Arrays replace wholesale regardless of element type.
        assertEquals(Set.of("events"), UpdateHelper.coveringFieldPaths(doc));
    }
}
