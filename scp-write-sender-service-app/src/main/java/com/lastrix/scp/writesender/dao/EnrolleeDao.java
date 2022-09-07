package com.lastrix.scp.writesender.dao;

import com.lastrix.scp.writesender.model.EnrolleeSelect;
import com.lastrix.scp.writesender.model.EnrolleeSelectId;

import java.util.List;

public interface EnrolleeDao {
    int commit(List<EnrolleeSelect> changes);

    List<EnrolleeSelect> fetch(int page);

    int confirm(List<EnrolleeSelectId> list);
}
