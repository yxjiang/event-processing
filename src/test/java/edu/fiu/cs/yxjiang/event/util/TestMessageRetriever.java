package edu.fiu.cs.yxjiang.event.util;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class TestMessageRetriever {
  
  @Test
  public void testGet() {
    JsonObject level3 = new JsonObject();
    level3.add("level4", new JsonPrimitive("inner most"));
    JsonObject level2 = new JsonObject();
    level2.add("level3", level3);
    
    JsonObject level32 = new JsonObject();
    level32.add("level42", new JsonPrimitive("inner most 2"));
    JsonObject level22 = new JsonObject();
    level22.add("level32", level32);
    
    
    JsonArray arr = new JsonArray();
    arr.add(level3);
    arr.add(level32);
    
    JsonObject level1 = new JsonObject();
    level1.add("level2", new JsonPrimitive("level2"));
    level1.add("arr", arr);
    
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    System.out.println(gson.toJson(level1));
    System.out.println();
    System.out.println(gson.toJson(MessageRetriever.get("level2", level1)));
    System.out.println();
    System.out.println(gson.toJson(MessageRetriever.get("arr[1].level42", level1)));
  }

}
