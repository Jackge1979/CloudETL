package com.dataliance.analysis.data.region;

import java.util.*;
import com.dataliance.analysis.data.region.model.*;

public interface RegionDistributionQuery
{
    List<RegionDistributionModel> getRegionDistributionByParamater(final Map<String, String> p0);
}
