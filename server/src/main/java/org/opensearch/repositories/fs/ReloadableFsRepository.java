/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.RepositoryException;

import java.nio.file.Path;

public class ReloadableFsRepository extends FsRepository {
    private static final Logger logger = LogManager.getLogger(ReloadableFsRepository.class);

    /**
     * Constructs a shared file system repository.
     *
     * @param metadata
     * @param environment
     * @param namedXContentRegistry
     * @param clusterService
     * @param recoverySettings
     */
    public ReloadableFsRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings);
    }

    @Override
    public boolean isReloadable() {
        return true;
    }

    @Override
    public void reload(RepositoryMetadata repositoryMetadata, boolean compress) {
        super.reload(repositoryMetadata, compress);
        metadata = repositoryMetadata;

        // TODO - deduplicate the below block
        String location = REPOSITORIES_LOCATION_SETTING.get(metadata.settings());
        if (location.isEmpty()) {
            logger.warn(
                "the repository location is missing, it should point to a shared file system location"
                    + " that is available on all cluster-manager and data nodes"
            );
            throw new RepositoryException(metadata.name(), "missing location");
        }
        Path locationFile = environment.resolveRepoFile(location);
        if (locationFile == null) {
            if (environment.repoFiles().length > 0) {
                logger.warn(
                    "The specified location [{}] doesn't start with any " + "repository paths specified by the path.repo setting: [{}] ",
                    location,
                    environment.repoFiles()
                );
                throw new RepositoryException(
                    metadata.name(),
                    "location [" + location + "] doesn't match any of the locations specified by path.repo"
                );
            } else {
                logger.warn(
                    "The specified location [{}] should start with a repository path specified by"
                        + " the path.repo setting, but the path.repo setting was not set on this node",
                    location
                );
                throw new RepositoryException(
                    metadata.name(),
                    "location [" + location + "] doesn't match any of the locations specified by path.repo because this setting is empty"
                );
            }
        }

        if (CHUNK_SIZE_SETTING.exists(metadata.settings())) {
            chunkSize = CHUNK_SIZE_SETTING.get(metadata.settings());
        } else {
            this.chunkSize = REPOSITORIES_CHUNK_SIZE_SETTING.get(environment.settings());
        }

        final String path = BASE_PATH_SETTING.get(metadata.settings());
        if (Strings.hasLength(path)) {
            basePath = new BlobPath().add(path);
        } else {
            this.basePath = BlobPath.cleanPath();
        }
    }
}
