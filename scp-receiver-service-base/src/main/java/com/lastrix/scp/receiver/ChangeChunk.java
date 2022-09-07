package com.lastrix.scp.receiver;

import java.util.List;

public record ChangeChunk<T>(List<T> changes, Object slab) {
    public boolean isEmpty() {
        return changes == null || changes.isEmpty();
    }
}
