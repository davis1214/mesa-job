package com.di.monitor.alarm.util;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.di.mesa.metric.model.MAlarm;
import com.di.mesa.metric.util.GsonUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Davi on 17/9/25.
 */
public class JsonUtilTest {

    private transient Gson mapper;

    @Test
    public void testMAlarmConvert() {

        // {
        // "malarmConditions": {
        // "type": 1,
        // "period": 1,
        // "slideType": 1,
        // "threshold": "100"
        // },
        // "mAlarmId": "di_mesa_kudu_order",
        // "alarmType": 2,
        // "alarmFrequency": 60,
        // "alarmCount": 0,
        // "alarmTriggerCount": "1",
        // "isAlarm": 1,
        // "connectionType": "and"
        // }

        String json = " {" + "    \"malarmConditions\": [{" + "        \"type\": 1," + "        \"period\": 1,"
                + "        \"slideType\": 1," + "        \"threshold\": \"100\"" + "    }],"
                + "    \"mAlarmId\": \"di_mesa_kudu_order\"," + "    \"alarmType\": 2," + "    \"alarmFrequency\": 60,"
                + "    \"alarmCount\": 0," + "    \"alarmTriggerCount\": \"1\"," + "    \"isAlarm\": 1,"
                + "    \"connectionType\": \"and\"" + "}";

        System.out.println(json);

        GsonBuilder builder = new GsonBuilder();
        mapper = builder.create();

        try {

//			JsonParser parser = new JsonParser();
//			JsonObject jsonObject = parser.parse(json).getAsJsonObject();

            MAlarm mAlarm = mapper.fromJson(json, new TypeToken<MAlarm>() {
            }.getType());

            System.out.println(mAlarm);


            Assert.assertTrue(mAlarm.getmAlarmId().equals("di_mesa_kudu_order"));
            Assert.assertTrue(mAlarm.getMalarmConditions().size() ==1);

        } catch (Exception e) {
            e.printStackTrace();
        }

        Map<String, ?> map = GsonUtil.toMap(json);

        System.out.println(map);


    }

    @Test
    public void testJsonConstruct() {

        Map<String, Object> map = Maps.newHashMap();

        map.put("metric", "di-mesa-bhv");

        Map<String, String> tags = Maps.newHashMap();
        tags.put("ExecuteCost", "ExecuteCost");
        tags.put("TupleCount", "TupleCount");
        map.put("tags", tags);

        List<Map<String, String>> alarmsList = new ArrayList<>();

        // Type: 2
        // Period: 0
        // Slide_Type: 0
        // Threshold: 1.0
        Map<String, String> oneAlarm = Maps.newHashMap();
        oneAlarm.put("Type", "2");
        oneAlarm.put("Period", "0");
        oneAlarm.put("Slide_Type", "0");
        oneAlarm.put("Threshold", "1.0");
        alarmsList.add(oneAlarm);

        Map<String, String> twoAlarm = Maps.newHashMap();
        twoAlarm.put("Type", "3");
        twoAlarm.put("Period", "0");
        twoAlarm.put("Slide_Type", "0");
        twoAlarm.put("Threshold", "30");
        alarmsList.add(twoAlarm);

        map.put("alarms", alarmsList);

        // out.println(JsonUtils.toJson(map));

    }

}
