package com.lastrix.scp.sender;

import java.util.List;

public interface ChangeSourceService<T> {
    List<T> fetch(int page);

    void commit(List<T> changes);
}
