package org.opensearch.index.engine.dataformat.commit;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.Store;

@ExperimentalApi
public interface CommitterFactory {
    Committer getCommitter(Store store);
}
