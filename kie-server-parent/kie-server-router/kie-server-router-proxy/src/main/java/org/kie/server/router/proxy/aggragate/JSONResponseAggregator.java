/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.kie.server.router.proxy.aggragate;

import com.google.gson.*;

import static org.kie.server.router.utils.Helper.readProperties;

import java.util.*;

public class JSONResponseAggregator implements ResponseAggregator {

    private static final String JSON_TYPE = "application/json";

    private static final Properties sortByMapping = readProperties(JSONResponseAggregator.class.getResourceAsStream("/sort-json.mapping"));

    public String aggregate(List<String> data) {

        return aggregate(data, null, true, 0, 10);
    }


    @Override
    public String aggregate(List<String> data, String sortBy, boolean ascending, Integer page, Integer pageSize) {

        try {
            JsonObject json = data.stream().map(s -> {
                return newJson(s);
            }).reduce((source, target) -> {
                deepMerge(source, target);
                return target;
            }).get();

            String response = sort(sortBy, ascending, page, pageSize, json);

            return response;
        } catch (IllegalStateException e) {
            // try with sorting array

            JsonArray jsonArray = data.stream().map(s -> {
                return newJsonArray(s);
            }).reduce((source, target) -> {
                deepMergeArray(source, target);
                return target;
            }).get();

            String response = sortArray(sortBy, ascending, page, pageSize, jsonArray);

            return response;
        }

    }

    protected String sort(String fieldName, boolean ascending, Integer page, Integer pageSize, JsonObject source) {
        try {
            for (String key: source.keySet()){
                Object value = source.get(key);
                if (value instanceof JsonArray) {
                    JsonArray array = (JsonArray) value;
                    // apply sorting
                    JsonArray jsonArray = sortList(fieldName, array, ascending, page, pageSize);
                    //remove old and add new sorted
                    source.remove(key);
                    source.add(key,jsonArray);
                }
            }

            return source.toString();
        } catch (Exception e) {
            throw new RuntimeException("Error while sorting and paging of json", e);
        }
    }

    protected JsonObject deepMerge(JsonObject source, JsonObject target) {
        try {
            for (String key: source.keySet()){//JSONObject.getNames(source)) {
                Object value = source.get(key);
                if (!target.has(key)) {
                    // new value for "key":
                    target.add(key, (JsonElement) value);
                } else {
                    // existing value for "key" - recursively deep merge:
                    if (value instanceof JsonObject) {
                        JsonObject valueJson = (JsonObject)value;
                        deepMerge(valueJson, target.get(key).getAsJsonObject());
                    }
                    // insert each JSONArray's JSONObject in place
                    else if (value instanceof JsonArray) {
                        JsonArray jsonArray = ((JsonArray) value);
                        for (int i = 0, size = jsonArray.size(); i < size; i++) {
                            JsonObject objectInArray = jsonArray.get(i).getAsJsonObject();
                            ((JsonArray) target.get(key)).add(objectInArray);
                        }
                    } else {
                        target.add(key, (JsonElement) value);
                    }
                }
            }
            return target;
        } catch (JsonIOException e) {
            return null;
        }
    }

    protected JsonArray deepMergeArray(JsonArray source, JsonArray target) {
        try {

            for (int i = 0, size = source.size(); i < size; i++) {
                JsonArray objectInArray = source.get(i).getAsJsonArray();
                target.add(objectInArray);
            }
            return target;
        } catch (JsonIOException e) {
            return null;
        }
    }

    protected String sortArray(String fieldName, boolean ascending, Integer page, Integer pageSize, JsonArray source) {
        try {
            // apply sorting
            source = sortList(fieldName, source, ascending, page, pageSize);

            return source.toString();
        } catch (Exception e) {
            throw new RuntimeException("Error while sorting and paging of json", e);
        }
    }

    protected JsonObject newJson(String data) {
        try {

            return JsonParser.parseString(data).getAsJsonObject();
        } catch (JsonIOException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    protected JsonArray newJsonArray(String data) {
        try {

            return JsonParser.parseString(data).getAsJsonArray();
        } catch (JsonIOException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public boolean supports(Object... acceptTypes) {
        for (Object acceptType : acceptTypes ) {
            if (acceptType == null) {
                continue;
            }
            boolean found = acceptType.toString().toLowerCase().contains(JSON_TYPE);
            if (found) {
                return true;
            }
        }

        return false;
    }

    protected JsonArray sortList(String fieldName, JsonArray array, boolean ascending, int page, int pageSize) throws Exception{

        Iterator<JsonElement> jsonElementIterator= array.iterator();
        List<JsonElement> jsonObjects=new ArrayList<>();
        jsonElementIterator.forEachRemaining(jsonElement -> {
            if(jsonElement instanceof JsonObject){
                jsonObjects.add(jsonElement.getAsJsonObject());
            }
            else{
                jsonObjects.add(jsonElement.getAsJsonArray());
            }
        });

        if (fieldName != null && !fieldName.isEmpty()) {
            String sortBy = sortByMapping.getProperty(fieldName, fieldName);


            Collections.sort(jsonObjects, new Comparator<JsonElement>() {

                @SuppressWarnings({"rawtypes", "unchecked"})
                @Override
                public int compare(JsonElement o1, JsonElement o2) {
                    if (o1 instanceof JsonObject && o2 instanceof JsonObject) {
                        try {
                            Comparable v1 = (Comparable<?>)((JsonObject) o1).get(sortBy).getAsString();
                            Comparable v2 = (Comparable<?>)((JsonObject) o2).get(sortBy).getAsString();
                            if (ascending) {
                                return v1.compareTo(v2);
                            } else {
                                return v2.compareTo(v1);
                            }
                        } catch (Exception e) {

                        }
                    }
                    else if (o1 instanceof JsonArray && o2 instanceof JsonArray) {
                        try {
                            Comparable v1 = (Comparable<?>)((JsonArray) o1).get(0).getAsString();
                            Comparable v2 = (Comparable<?>)((JsonArray) o2).get(0).getAsString();
                            if (ascending) {
                                return v1.compareTo(v2);
                            } else {
                                return v2.compareTo(v1);
                            }
                        } catch (Exception e) {

                        }
                    }

                    return 0;
                }
            });
        }

        // calculate paging
        int start = page * pageSize;
        int end = start + pageSize;
        // apply paging
        if (jsonObjects.size() < start) {
            // no elements in given range, return empty
            jsonObjects.clear();
        } else if (jsonObjects.size() >= end) {
            List<?> tmp = jsonObjects.subList(start, end);
            jsonObjects.retainAll(tmp);
        } else if (jsonObjects.size() < end) {
            List<?> tmp = jsonObjects.subList(start, jsonObjects.size());
            jsonObjects.retainAll(tmp);
        }

        JsonArray jsonArray = new JsonArray();
        jsonObjects.forEach(jsonObject -> jsonArray.add(jsonObject));
        return jsonArray;
    }

}

