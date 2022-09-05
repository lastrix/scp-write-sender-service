package com.lastrix.scp.writesender.service;

import com.lastrix.scp.writesender.base.ChangeSender;
import com.lastrix.scp.writesender.base.ChangeSenderService;
import com.lastrix.scp.writesender.base.ChangeSourceService;
import com.lastrix.scp.writesender.model.EnrolleeSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class DefaultChangeSenderService extends ChangeSenderService<EnrolleeSelect> {
    private static final Logger log = LoggerFactory.getLogger(DefaultChangeSenderService.class);
    private final int channelStart;
    private final int channelEnd;
    private final int channelMask;

    @Autowired
    public DefaultChangeSenderService(
            ChangeSourceService<EnrolleeSelect> source,
            ChangeSender<EnrolleeSelect> sender,
            @Value("${scp.writesender.worker.parallelism:4}") int parallelism,
            @Value("${scp.writesender.worker.channels:8}") int channels,
            @Value("${scp.writesender.worker.max-processing-chunk:1024}") int maxProcessingChunk,
            @Value("${scp.writesender.worker.channels.start}") int channelStart,
            @Value("${scp.writesender.worker.channels.end}") int channelEnd,
            @Value("${scp.writesender.worker.channels.mask}") int channelMask) {
        super(source, sender, parallelism, channels, maxProcessingChunk);
        this.channelStart = channelStart;
        this.channelEnd = channelEnd;
        this.channelMask = channelMask;
    }

    @Override
    protected Object idOf(EnrolleeSelect o) {
        return new EnrolleeSelectId(o);
    }

    @Override
    protected int channelOf(EnrolleeSelect o) {
        var channel = (int) o.getSpecId().getLeastSignificantBits() & channelMask;
        if (channel < channelStart || channel >= channelEnd) {
            log.warn("Wrong channel usage detected in database for {}:{}:{}", o.getUserId(), o.getSessionId(), o.getSpecId());
        }
        return channel;
    }

    private static final class EnrolleeSelectId {
        private final UUID userId;
        private final int sessionId;
        private final UUID specId;
        private final int ordinal;

        public EnrolleeSelectId(EnrolleeSelect s) {
            userId = s.getUserId();
            sessionId = s.getSessionId();
            specId = s.getSpecId();
            ordinal = s.getOrdinal();
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
}
