/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.upgrade.cli.parser;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for parsing Oak Segment Azure configuration (e.g. connection
 * string, container name, uri, etc.) from custom encoded String or Azure
 * standard URI.
 */
public class AzureParserUtils {
    private static final String PREFIX_DEFAULT_ENDPOINTS_PROTOCOL = "DefaultEndpointsProtocol";
    private static final String PREFIX_CONTAINER_NAME = "ContainerName=";
    private static final String PREFIX_DIRECTORY = "Directory=";
    
    public static final String KEY_CONNECTION_STRING = "connectionString";
    public static final String KEY_CONTAINER_NAME = "containerName";
    public static final String KEY_ACCOUNT_NAME = "accountName";
    public static final String KEY_STORAGE_URI = "storageUri";
    public static final String KEY_DIR = "dir";

    private AzureParserUtils() {
        // prevent instantiation
    }

    /**
     * 
     * @param conn
     *            the connection string
     * @return <code>true</code> if this is a custom encoded Azure connection
     *         String, <code>false</code> otherwise
     */
    public static boolean isCustomAzureConnectionString(String conn) {
        return conn.contains(PREFIX_DEFAULT_ENDPOINTS_PROTOCOL);
    }

    /**
     * Parses a custom encoded connection string of the form (line breaks added for
     * clarity): 
     * <br><br>
     * <b>DefaultEndpointsProtocol</b>=http;<b>AccountName</b>=devstoreaccount1;
     * <b>AccountKey</b>=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;
     * <b>BlobEndpoint</b>=http://127.0.0.1:10000/devstoreaccount1; <br>
     * <b>ContainerName</b>=mycontainer; <br>
     * <b>Directory</b>=mydir 
     * <br><br>
     * where the first three lines in the string represent a standard Azure
     * Connection String and the last two lines are Oak Segment Azure specific
     * arguments.
     * 
     * @param conn
     *            the connection string
     * @return parsed configuration map containing the Azure <b>connectionString</b>,
     *         <b>containerName</b> and <b>dir</b> (key names in bold)
     */
    public static Map<String, String> parseAzureConfigurationFromCustomConnection(String conn) {
        Map<String, String> config = new HashMap<>();

        int containerNameIndex = conn.indexOf(PREFIX_CONTAINER_NAME);
        int dirIndex = conn.indexOf(PREFIX_DIRECTORY);

        String connectionString = conn.substring(0, containerNameIndex);
        String containerName = conn.substring(containerNameIndex + PREFIX_CONTAINER_NAME.length(), dirIndex - 1);
        String dir = conn.substring(dirIndex + PREFIX_DIRECTORY.length());

        config.put(KEY_CONNECTION_STRING, connectionString);
        config.put(KEY_CONTAINER_NAME, containerName);
        config.put(KEY_DIR, dir);

        return config;
    }

    /**
     * Parses a standard Azure URI in the format
     * <b>https</b>://<b>myaccount</b>.blob.core.windows.net/<b>container</b>/<b>repo</b>,
     * 
     * @param uri
     *            the Azure URI
     * @return parsed configuration map containing <b>accountName</b>, <b>storageUri</b> and <b>dir</b>
     * (key names in bold)
     */
    public static Map<String, String> parseAzureConfigurationFromUri(String uri) {
        Map<String, String> config = new HashMap<>();

        int lastSlashPos = uri.lastIndexOf('/');
        int doubleSlashPos = uri.indexOf("//");
        int firstDotPos = uri.indexOf(".");

        String accountName = uri.substring(doubleSlashPos + 2, firstDotPos);
        String storageUri = uri.substring(0, lastSlashPos);
        String dir = uri.substring(lastSlashPos + 1);

        config.put(KEY_ACCOUNT_NAME, accountName);
        config.put(KEY_STORAGE_URI, storageUri);
        config.put(KEY_DIR, dir);

        return config;
    }
}
