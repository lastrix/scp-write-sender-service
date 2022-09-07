package com.lastrix.scp.writesender.service;

import com.lastrix.scp.receiver.ChangeReceiver;
import com.lastrix.scp.receiver.ChangeReceiverService;
import com.lastrix.scp.receiver.ChangeSinkService;
import com.lastrix.scp.writesender.model.EnrolleeSelectId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class DefaultChangeReceiverService extends ChangeReceiverService<EnrolleeSelectId> {
    public DefaultChangeReceiverService(
            ChangeSinkService<EnrolleeSelectId> sink,
            ChangeReceiver<EnrolleeSelectId> receiver,
            @Value("${scp.wss.confirm.chunk-size}") int sinkChunkSize,
            @Value("${scp.wss.confirm.max-chunk-size}") int maxSinkChunkSize,
            @Value("${scp.wss.confirm.receive-buffer-size}") int receiveBufferSize) {
        super(sink, receiver, sinkChunkSize, maxSinkChunkSize, receiveBufferSize);
    }

    @Override
    protected Object idOf(EnrolleeSelectId c) {
        return c;
    }
}
