package org.opensearch.index.engine.dataformat.commit;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;

@ExperimentalApi
public interface Committer {

    Map<String, String> readLastCommittedUserData();

    long getLastCommittedGeneration();
}
