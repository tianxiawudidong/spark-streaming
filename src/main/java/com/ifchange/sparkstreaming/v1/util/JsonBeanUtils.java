package com.ifchange.sparkstreaming.v1.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonBeanUtils {

    private static Logger logger = LoggerFactory.getLogger(JsonBeanUtils.class);

    private ObjectMapper mapper;


    public JsonBeanUtils() {
        mapper = new ObjectMapper();
    }

    /**
     * CV 转json string
     *
     * @param cv
     * @return
     */
    public String objectToJson(Object cv) {
        String json = "";
        // null替换为""
        mapper.getSerializerProvider().setNullValueSerializer(new JsonSerializer<Object>() {
            @Override
            public void serialize(Object arg0, JsonGenerator arg1, SerializerProvider arg2) throws IOException, JsonProcessingException {
                arg1.writeString("");
            }
        });
        try {
            json = mapper.writeValueAsString(cv);
        } catch (JsonProcessingException e) {
            logger.warn("json parse error", e.getMessage());
        }
        return json;
    }


}
