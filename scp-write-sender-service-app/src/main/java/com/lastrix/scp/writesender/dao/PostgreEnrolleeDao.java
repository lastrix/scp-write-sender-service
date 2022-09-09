package com.lastrix.scp.writesender.dao;

import com.lastrix.scp.writesender.model.EnrolleeSelect;
import com.lastrix.scp.writesender.model.EnrolleeSelectId;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Slf4j
@Repository
public class PostgreEnrolleeDao implements EnrolleeDao {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public PostgreEnrolleeDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public int commit(List<EnrolleeSelect> changes) {
        int[] a = jdbcTemplate.batchUpdate(
                "UPDATE scp_write_service.enrollee_select SET state = 1, modified_stamp = CURRENT_TIMESTAMP WHERE user_id = ? AND session_id = ? AND spec_id = ? AND ordinal = ?",
                changes.stream().map(x -> new Object[]{x.getUserId(), x.getSessionId(), x.getSpecId(), x.getOrdinal()}).toList()
        );
        return sumArray(a);
    }

    @Override
    public int confirm(List<EnrolleeSelectId> list) {
        int[] a = jdbcTemplate.batchUpdate(
                "UPDATE scp_write_service.enrollee_select SET state = 2, modified_stamp = CURRENT_TIMESTAMP WHERE user_id = ? AND session_id = ? AND spec_id = ? AND ordinal = ?",
                list.stream().map(x -> new Object[]{x.getUserId(), x.getSessionId(), x.getSpecId(), x.getOrdinal()}).toList()
        );
        return sumArray(a);
    }

    @Override
    public List<EnrolleeSelect> fetch(int page) {
        return jdbcTemplate.query(
                """
                        SELECT  user_id,
                                session_id,
                                spec_id,
                                status,
                                score,
                                created_stamp,
                                confirmed_stamp,
                                canceled_stamp,
                                ordinal
                        FROM scp_write_service.enrollee_select es
                        WHERE es.state = 0
                        ORDER BY es.modified_stamp
                        LIMIT 128
                        OFFSET ?""",
                (rs, rowNum) -> mapToEnrolleeSelect(rs),
                page * 128
        );
    }

    private EnrolleeSelect mapToEnrolleeSelect(ResultSet rs) throws SQLException {
        var r = new EnrolleeSelect();
        r.setUserId(UUID.fromString(rs.getString(1)));
        r.setSessionId(rs.getInt(2));
        r.setSpecId(UUID.fromString(rs.getString(3)));
        r.setStatus(rs.getShort(4));
        r.setScore(rs.getInt(5));
        r.setCreatedStamp(toInstantOrNull(rs, 6));
        r.setConfirmedStamp(toInstantOrNull(rs, 7));
        r.setCancelledStamp(toInstantOrNull(rs, 8));
        r.setOrdinal(rs.getShort(9));
        return r;
    }

    private Instant toInstantOrNull(ResultSet rs, int columnIndex) throws SQLException {
        Timestamp ts = rs.getTimestamp(columnIndex);
        return ts == null ? null : ts.toInstant();
    }

    private int sumArray(int[] a) {
        int t = 0;
        for (int v : a) {
            t += v;
        }
        return t;
    }
}
