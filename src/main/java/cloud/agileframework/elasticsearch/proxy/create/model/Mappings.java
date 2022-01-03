package cloud.agileframework.elasticsearch.proxy.create.model;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.util.Map;

@Data
public class Mappings {
    private Map<String, JSONObject> properties;

    public Mappings(Map<String, JSONObject> properties) {
        this.properties = properties;
    }
}
