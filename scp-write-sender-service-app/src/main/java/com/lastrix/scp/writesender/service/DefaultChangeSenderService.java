package com.lastrix.scp.writesender.service;

import com.lastrix.scp.sender.ChangeSender;
import com.lastrix.scp.sender.ChangeSenderService;
import com.lastrix.scp.sender.ChangeSourceService;
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
            @Value("${scp.wss.worker.parallelism}") int parallelism,
            @Value("${scp.wss.worker.channels.count}") int channels,
            @Value("${scp.wss.worker.max-processing-chunk}") int maxProcessingChunk,
            @Value("${scp.wss.worker.channels.start}") int channelStart,
            @Value("${scp.wss.worker.channels.end}") int channelEnd,
            @Value("${scp.wss.worker.channels.mask}") int channelMask) {
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
            // we should not throw errors here, just warn log that something bad happens with database
            // or our configuration
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
