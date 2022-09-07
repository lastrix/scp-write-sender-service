package com.lastrix.scp.sender;

import java.util.List;

public interface ChangeSender<T> {
    List<T> send(List<T> changes, int channel);
}
