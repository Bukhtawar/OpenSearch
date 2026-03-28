/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.checksum;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that maps data format names to their {@link ChecksumHandler} implementations.
 *
 * <p>Built-in: {@code "lucene"} → {@link LuceneChecksumHandler} (always registered).</p>
 * <p>Fallback: {@link GenericCRC32ChecksumHandler} for any unregistered format.</p>
 *
 * <p>TODO: When DataSourcePlugin is implemented, plugins will provide custom handlers
 * via {@code DataSourcePlugin.getChecksumHandler()} and they will be registered here
 * by {@code DefaultCompositeStoreDirectoryFactory}.</p>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class ChecksumHandlerRegistry {

    private static final Logger logger = LogManager.getLogger(ChecksumHandlerRegistry.class);

    private final Map<String, ChecksumHandler> handlers = new ConcurrentHashMap<>();
    private final ChecksumHandler defaultHandler = new GenericCRC32ChecksumHandler();

    /**
     * Creates a new registry with the built-in Lucene handler pre-registered.
     */
    public ChecksumHandlerRegistry() {
        registerHandler(new LuceneChecksumHandler());
        logger.debug("ChecksumHandlerRegistry initialized with built-in Lucene handler");
    }

    /**
     * Registers a checksum handler for a specific format.
     *
     * @param handler the checksum handler to register
     */
    public void registerHandler(ChecksumHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("ChecksumHandler cannot be null");
        }
        handlers.put(handler.getFormatName(), handler);
        logger.debug("Registered checksum handler for format: {} ({})", handler.getFormatName(), handler.getClass().getSimpleName());
    }

    /**
     * Returns the handler for the given format, or the default CRC32 handler if none registered.
     */
    public ChecksumHandler getHandler(String formatName) {
        return handlers.getOrDefault(formatName, defaultHandler);
    }

    /**
     * Calculates checksum using the appropriate format-specific handler.
     */
    public long calculateChecksum(String formatName, IndexInput input) throws IOException {
        return getHandler(formatName).calculateChecksum(input);
    }

    /**
     * Calculates upload checksum using the appropriate format-specific handler.
     */
    public String calculateUploadChecksum(String formatName, IndexInput input) throws IOException {
        return getHandler(formatName).calculateUploadChecksum(input);
    }

    public Set<String> getRegisteredFormats() {
        return Collections.unmodifiableSet(handlers.keySet());
    }

    public boolean hasHandler(String formatName) {
        return handlers.containsKey(formatName);
    }
}
