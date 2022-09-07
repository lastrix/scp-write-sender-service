package com.lastrix.scp.receiver;

import java.util.List;

public interface ChangeSinkService<T> {
    /**
     * Commit selected changes to storage
     *
     * @param changes the list of changes to store
     */
    void commit(List<T> changes);
}
