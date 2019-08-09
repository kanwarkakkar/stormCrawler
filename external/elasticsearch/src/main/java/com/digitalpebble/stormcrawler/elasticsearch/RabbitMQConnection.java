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

package com.digitalpebble.stormcrawler.elasticsearch;

import java.util.Map;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * Utility class to instantiate an RMQ client;
 **/
public class RabbitMQConnection {

	 private Channel ChannelC;
	private RabbitMQConnection(Channel c) {
		ChannelC = c;
    }
	 public Channel getClient() {
	        return ChannelC;
	  }
	
	public static RabbitMQConnection getChannel(Map stormConf) {
    	Channel channelRMQ = null;
        ConnectionFactory factory = new ConnectionFactory();
        String hostName = ConfUtils.getString(stormConf, "rmq.host","localhost");
        String virtualHost = ConfUtils.getString(stormConf, "rmq.virtualhost","defaultvHost");
        String queueName = ConfUtils.getString(stormConf, "rmq.queueName","defaultQueue");
        
    	factory.setHost(hostName);
    	factory.setVirtualHost(virtualHost);
    	try{
    		Connection connection = factory.newConnection();
    		channelRMQ = connection.createChannel();
    		channelRMQ.queueDeclare(queueName, true, false, false, null);
    	}catch(Exception e) {
    	}
        return new RabbitMQConnection(channelRMQ);
    }


    public void close() {
      
    }
}
