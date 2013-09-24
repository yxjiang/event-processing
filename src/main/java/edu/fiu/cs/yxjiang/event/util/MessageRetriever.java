package edu.fiu.cs.yxjiang.event.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Retrieve the meta-data from Message.
 * @author yjian004
 *
 */
public class MessageRetriever {
  
  /**
   * Retrieve the meta-data from the json element.
   * @param path
   * @param elem
   * @return
   */
  public static JsonElement get(String path, JsonElement elem) {
    String[] tokens = path.trim().split("\\.");
    
    if (tokens.length == 0) {
      tokens = new String[] { path };
    }
    
    JsonElement current = elem;
    
    for (int i = 0; i < tokens.length; ++i) {
      if (!tokens[i].contains("[")) { // retrieve object
        if (!(current instanceof JsonObject)) {
          throw new IllegalStateException("Invalid path.");
        }
        current = ((JsonObject)current).get(tokens[i]);
      }
      else { // retrieve array element
        if (!(current instanceof JsonObject)) {
          throw new IllegalStateException("Invalid path.");
        }
        int leftParenthesis = tokens[i].indexOf("[");
        int rightParenthesis = tokens[i].indexOf("]");
        if (leftParenthesis == -1 || rightParenthesis == -1 || leftParenthesis >= rightParenthesis) {
          throw new IllegalStateException("Invalid path.");
        }
        String key = tokens[i].substring(0, leftParenthesis);
        try {
          int index = Integer.parseInt(tokens[i].substring(leftParenthesis + 1, rightParenthesis));
          current = ((JsonObject)current).get(key);
          
          if (!(current instanceof JsonArray)) {
            throw new IllegalStateException("Invalid path.");
          }
          
          current = ((JsonArray)current).get(index);
        } catch (NumberFormatException e) {
          throw new IllegalStateException("Invalid path.");
        }
      }
    }
    
    return current;
  }

}
