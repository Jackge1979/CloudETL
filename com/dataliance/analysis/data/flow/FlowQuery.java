package com.dataliance.analysis.data.flow;

import java.util.*;
import com.dataliance.analysis.data.flow.model.*;

public interface FlowQuery
{
    List<FlowModel> getFlowModelListByParam(final Map<String, String> p0);
}
