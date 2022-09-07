package com.lastrix.scp.writesender.service;

import com.lastrix.scp.receiver.ChangeSinkService;
import com.lastrix.scp.writesender.dao.EnrolleeDao;
import com.lastrix.scp.writesender.model.EnrolleeSelectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DefaultChangeSinkService implements ChangeSinkService<EnrolleeSelectId> {
    private static final Logger log = LoggerFactory.getLogger(DefaultChangeSinkService.class);
    private final EnrolleeDao dao;

    public DefaultChangeSinkService(EnrolleeDao dao) {
        this.dao = dao;
    }

    @Override
    public void commit(List<EnrolleeSelectId> changes) {
        int c = dao.confirm(changes);
        if (c != changes.size()) {
            log.info("Fewer records updated than expected: {} of {}", c, changes.size());
        }
    }
}
