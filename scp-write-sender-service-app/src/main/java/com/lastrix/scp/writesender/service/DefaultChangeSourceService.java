package com.lastrix.scp.writesender.service;

import com.lastrix.scp.writesender.base.ChangeSourceService;
import com.lastrix.scp.writesender.dao.EnrolleeDao;
import com.lastrix.scp.writesender.model.EnrolleeSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class DefaultChangeSourceService implements ChangeSourceService<EnrolleeSelect> {
    private static final Logger log = LoggerFactory.getLogger(DefaultChangeSourceService.class);

    private final EnrolleeDao dao;

    public DefaultChangeSourceService(EnrolleeDao dao) {
        this.dao = dao;
    }

    @Transactional(readOnly = true)
    @Override
    public List<EnrolleeSelect> fetch(int page) {
        return dao.fetch(page);
    }

    @Transactional
    @Override
    public void commit(List<EnrolleeSelect> changes) {
        // we may change fewer entries than expected
        // because of modification
        var c = dao.commit(changes);
        if (c != changes.size()) {
            log.trace("Fewer changes than expected: {}, should be {}", c, changes.size());
        }
    }
}
