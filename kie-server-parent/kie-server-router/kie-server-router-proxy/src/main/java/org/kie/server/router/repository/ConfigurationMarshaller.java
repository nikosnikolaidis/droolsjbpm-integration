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

package org.kie.server.router.repository;

import java.io.Reader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import org.kie.server.router.Configuration;
import org.kie.server.router.ContainerInfo;

public class ConfigurationMarshaller {

    public String marshall(Configuration configuration) throws Exception {
        Map<String, List<String>> perContainer = configuration.getHostsPerContainer();
        Map<String, List<String>> perServer = configuration.getHostsPerServer();
        Map<String, List<ContainerInfo>> containerInfo = configuration.getContainerInfosPerContainer();

        JsonArray servers = new JsonArray();
        JsonArray containers = new JsonArray();
        JsonArray infos = new JsonArray();
        JsonObject config = new JsonObject();
        
        for (Entry<String, List<String>> entry : perContainer.entrySet()) {
            JsonArray array = new JsonArray();
            entry.getValue().forEach(url -> array.add(url));
            
            JsonObject container = new JsonObject();
            container.add(entry.getKey(), array);
            
            containers.add(container);
        }
        
        for (Entry<String, List<String>> entry : perServer.entrySet()) {
            JsonArray array = new JsonArray();
            entry.getValue().forEach(url -> array.add(url));
            
            JsonObject server = new JsonObject();
            server.add(entry.getKey(), array);
            
            servers.add(server);
        }
        Set<String> processed = new HashSet<>();
        for (Entry<String, List<ContainerInfo>> entry : containerInfo.entrySet()) {
            if (processed.contains(entry.getKey())) {
                continue;
            }
            entry.getValue().forEach(ci -> {
                JsonObject jsonCI = new JsonObject();
                processed.add(ci.getAlias());
                processed.add(ci.getContainerId());
                try {
                    jsonCI.addProperty("alias", ci.getAlias());
                    jsonCI.addProperty("containerId", ci.getContainerId());
                    jsonCI.addProperty("releaseId", ci.getReleaseId());
                    infos.add(jsonCI);
                } catch (JsonIOException e) {
                    e.printStackTrace();
                }

            });
        }
        
        config.add("containers", containers);
        config.add("servers", servers);
        config.add("containerInfo", infos);
        
        return config.toString();
    }
    
    public Configuration unmarshall(Reader reader) throws Exception {
        Configuration configuration = new Configuration();
        JsonReader jsonReader = new JsonReader(reader);
        JsonObject config = JsonParser.parseReader(jsonReader).getAsJsonObject();
        
        JsonArray containers = config.get("containers").getAsJsonArray();
        for (int i = 0; i < containers.size(); i++) {
            JsonObject container = containers.get(i).getAsJsonObject();

            for (String name : container.keySet()) {
                JsonArray urls = (JsonArray) container.get(name);
                if (urls.size() > 0) {
                    for (int j = 0; j < urls.size(); j++) {
                        String url = urls.get(j).getAsString();

                        configuration.addContainerHost(name, url);
                    }
                } else {
                    configuration.addEmptyContainerHost(name);
                }
            }
        }

        JsonArray servers = config.get("servers").getAsJsonArray();
        for (int i = 0; i < servers.size(); i++) {
            JsonObject server = (JsonObject) servers.get(i);
            
            for (String name : server.keySet()) {
                JsonArray urls = (JsonArray) server.get(name);
                if (urls.size() > 0) {
                    for (int j = 0; j < urls.size(); j++) {
                        String url = urls.get(j).getAsString();

                        configuration.addServerHost(name, url);
                    }
                } else {
                    configuration.addEmptyServerHost(name);
                }
            }
        }

        JsonArray containerInfo = config.get("containerInfo").getAsJsonArray();
        for (int i = 0; i < containerInfo.size(); i++) {
            JsonObject info = (JsonObject) containerInfo.get(i);

            ContainerInfo actualInfo = new ContainerInfo(info.get("containerId").getAsString(), info.get("alias").getAsString(),
                    info.get("releaseId").getAsString());

            configuration.addContainerInfo(actualInfo);
        }
 
        return configuration; 
    }
}
