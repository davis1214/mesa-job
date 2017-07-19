package com.di.mesa.common.util;

import com.di.mesa.common.model.MapJ;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.script.ScriptEngine;  
import javax.script.ScriptEngineManager;  
import javax.script.ScriptException;  

public class ExprCaculateUtil {
	
    private static ScriptEngineManager factory = new ScriptEngineManager();  
    private static ScriptEngine engine = factory.getEngineByName("JavaScript");  
      
    public Double getMathValue(List<MapJ> map, String option){
        double d = 0;  
        try {  
            for(int i=0; i<map.size();i++){  
                MapJ mapj = map.get(i);  
                option = option.replaceAll(mapj.getKey(), mapj.getValue());  
            }  
            Object o = engine.eval(option);  
            d = Double.parseDouble(o.toString());  
        } catch (ScriptException e) {  
            System.out.println("无法识别表达式");  
            return null;  
        }  
        return d;  
    }
    
    public static Double getMathValue(Map<String, Double> mapj,String option){  
        double d = 0;  
        try {  
            for(Entry<String, Double> entry:mapj.entrySet()){  
                option = option.replaceAll(entry.getKey(), entry.getValue().toString());  
            }  
            Object o = engine.eval(option);  
            d = Double.parseDouble(o.toString());  
        } catch (ScriptException e) {  
            System.out.println("无法识别表达式");  
            return null;  
        }  
        return d;  
    }
    
    
}  
  
