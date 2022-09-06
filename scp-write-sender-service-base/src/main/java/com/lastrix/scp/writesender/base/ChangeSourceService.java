package com.lastrix.scp.writesender.base;

import java.util.List;

public interface ChangeSourceService<T> {
    List<T> fetch(int page);

    void commit(List<T> changes);
}
