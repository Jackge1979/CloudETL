package com.dataliance.analysis.data.pv;

import java.util.*;

import com.dataliance.analysis.data.pv.model.*;

public interface PVQuery
{
    List<PVModel> getPVModelList(final Map<String, String> p0);
}
