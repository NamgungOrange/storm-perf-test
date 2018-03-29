/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.storm.perftest;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CountBolt extends BaseRichBolt {
  private OutputCollector _collector;
  private static Map<String, ArrayList<String>> wordCountMap = new HashMap<String, ArrayList<String>>();
  private static Logger logger = LoggerFactory.getLogger(CountBolt.class);


  public CountBolt() {
    //Empty
  }

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    Map map = (Map) tuple.getValueByField("map");
    addMap2WordCountMap(map);
    Iterator iterator = map.keySet().iterator();
    logger.info("*****begin*****");
    while(iterator.hasNext()){
      String name = String.valueOf(iterator.next());
      logger.info(name+"\t"+((ArrayList<String>)map.get(name)).size()) ;
    }
    logger.info("*****end*****");


    _collector.emit(tuple, new Values(wordCountMap));
    _collector.ack(tuple);
  }

  private void addMap2WordCountMap(Map map) {
    String name = null;
    boolean isExist = false;
    if (map.keySet().iterator().hasNext()) {
      name = String.valueOf(map.keySet().iterator().next());
      synchronized (wordCountMap) {
        Iterator iterator = wordCountMap.keySet().iterator();
        while (iterator.hasNext()) {
          if (String.valueOf(iterator.next()).equals(name)) {
            wordCountMap.get(name).add(String.valueOf(map.get(name)));
            isExist = true;
          }
        }
        if (!isExist) {
          ArrayList<String> list = new ArrayList<String>();
          list.add(String.valueOf(map.get(name)));
          wordCountMap.put(name, list);
        }
      }

    }
  }
      @Override
  public void cleanup() {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("count"));
  }
}
