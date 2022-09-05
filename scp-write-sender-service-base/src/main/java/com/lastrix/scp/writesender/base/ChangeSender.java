package com.lastrix.scp.writesender.base;

import java.util.List;

public interface ChangeSender<T> {
    List<T> send(List<T> changes, int channel);
}
