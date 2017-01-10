/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.stormcrawler.filtering.host;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.bolt.FetcherBolt;
import com.digitalpebble.stormcrawler.filtering.URLFilter;
import com.fasterxml.jackson.databind.JsonNode;
import redis.clients.jedis.Jedis;

import crawlercommons.domains.PaidLevelDomain;

/**
 * Filters URL based on the hostname.
 * 
 * This filter has 2 modes:
 * <ul>
 * <li>if <code>ignoreOutsideHost</code> is <code>true</code>, all URLs with a
 * host different from the host of the source URL are filtered out</li>
 * <li>if <code>ignoreOutsideDomain</code> is <code>true</code>, all URLs with a
 * domain different from the source's domain are filtered out</li>
 * </ul>
 */
public class HostURLFilter implements URLFilter {
    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(HostURLFilter.class);
    private boolean ignoreOutsideHost;
    private boolean ignoreOutsideDomain;
    private Jedis jedis ;
    private URL previousSourceUrl;
    private String previousSourceHost;
    private String previousSourceDomain;

    @Override
    public void configure(Map stormConf, JsonNode filterParams) {
    	
        JsonNode filterByHostNode = filterParams.get("ignoreOutsideHost");
        if (filterByHostNode == null) {
            ignoreOutsideHost = false;
        } else {
            ignoreOutsideHost = filterByHostNode.asBoolean(false);
        }

        // ignoreOutsideDomain is not necessary if we require the host to be
        // always the same
        if (!ignoreOutsideHost) {
            JsonNode filterByDomainNode = filterParams
                    .get("ignoreOutsideDomain");
            if (filterByHostNode == null) {
                ignoreOutsideDomain = false;
            } else {
                ignoreOutsideDomain = filterByDomainNode.asBoolean(false);
            }
        } else {
            ignoreOutsideDomain = false;
        }
    }

    @Override
    public String filter(URL sourceUrl, Metadata sourceMetadata,
            String urlToFilter) {
    	
    	if (sourceUrl == null || (!ignoreOutsideHost && !ignoreOutsideDomain)) {
            return urlToFilter;
        }
    	jedis = new Jedis("localhost",6379);
        URL tURL;
        try {
            tURL = new URL(urlToFilter);
        } catch (MalformedURLException e1) {
            return null;
        }

        String fromHost;
        String fromDomain = null;
        // Using identity comparison because URL.equals performs poorly
        if (sourceUrl == previousSourceUrl) {
            fromHost = previousSourceHost;
            if (ignoreOutsideDomain) {
                fromDomain = previousSourceDomain;
            }
        } else {
            fromHost = sourceUrl.getHost();
            if (ignoreOutsideDomain) {
                fromDomain = PaidLevelDomain.getPLD(fromHost);
            }
            previousSourceHost = fromHost;
            previousSourceDomain = fromDomain;
            previousSourceUrl = sourceUrl;
        }

        // resolve the hosts
        String toHost = tURL.getHost();

        if (ignoreOutsideHost) {
            if (toHost == null || !toHost.equalsIgnoreCase(fromHost)) {
                return null;
            }
        }

        if (ignoreOutsideDomain) {
            String toDomain = PaidLevelDomain.getPLD(toHost);
            if (toDomain == null || !toDomain.equals(fromDomain)) {
                return null;
            }
        }
       
    	if(fromHost != null){
    		String urlCountStr = jedis.hget(fromHost,fromHost);
    		if(urlCountStr == null)
    		{
    			jedis.hset(fromHost, fromHost, "1");
    		}
            String urlCount = jedis.hget(fromHost,fromHost);
            String defaultLimit = jedis.get("defaultLimit");
            
            
            if(Integer.parseInt(urlCount)> Integer.parseInt(defaultLimit))
            {
            	 return null;
            	
            }else
            {
            	jedis.hincrBy(fromHost, fromHost, 1);
            }
            
        
    	}
    	  if(urlToFilter.contains("#")){
        	  if(fromHost != null){
    	    	{
    	    		if(jedis.exists(fromHost))
    	    		{
    	    			if(Integer.parseInt(jedis.hget(fromHost,fromHost)) > -1)
    	    				jedis.hincrBy(fromHost, fromHost, -1);
    	    		}
    	    	}
    	    	 return null;
        	  }
    	  }
    	
    

        return urlToFilter;
    }

}
