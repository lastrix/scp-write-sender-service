package com.lastrix.scp.writesender.base;

import java.time.Instant;
import java.util.List;

public interface ChangeSourceService<T> {
    List<T> getChanges(int page);

    void commitChanges(List<T> changes);
}
