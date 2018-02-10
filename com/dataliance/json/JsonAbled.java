package com.dataliance.json;

import com.dataliance.json.util.*;

import net.sf.json.*;

public abstract class JsonAbled implements JsonAble
{
    @Override
    public JSONObject toJson() throws Exception {
        return JsonUtil.toJson(this);
    }
}
