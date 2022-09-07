package com.lastrix.scp.writesender.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class EnrolleeSelectId {
    private UUID userId;
    private int sessionId;
    private UUID specId;
    private int ordinal;

    public EnrolleeSelectId() {
    }

    public EnrolleeSelectId(UUID userId, int sessionId, UUID specId, int ordinal) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.specId = specId;
        this.ordinal = ordinal;
    }

    public UUID getUserId() {
        return userId;
    }

    public void setUserId(UUID userId) {
        this.userId = userId;
    }

    public int getSessionId() {
        return sessionId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public UUID getSpecId() {
        return specId;
    }

    public void setSpecId(UUID specId) {
        this.specId = specId;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public void setOrdinal(int ordinal) {
        this.ordinal = ordinal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EnrolleeSelectId that)) return false;

        if (sessionId != that.sessionId) return false;
        if (ordinal != that.ordinal) return false;
        if (!userId.equals(that.userId)) return false;
        return specId.equals(that.specId);
    }

    @Override
    public int hashCode() {
        int result = userId.hashCode();
        result = 31 * result + sessionId;
        result = 31 * result + specId.hashCode();
        result = 31 * result + ordinal;
        return result;
    }
}
