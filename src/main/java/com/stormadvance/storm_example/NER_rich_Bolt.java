package com.stormadvance.storm_example;

import com.aliasi.chunk.Chunk;
import com.aliasi.chunk.Chunking;
import com.aliasi.crf.ChainCrfChunker;
import com.aliasi.util.AbstractExternalizable;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by a1 on 4/2/2017.
 */
public class NER_rich_Bolt extends BaseRichBolt {
    OutputCollector _collector;
    File modelFile ;
    ChainCrfChunker crfChunker;


    private String row;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        modelFile = new File("model.crf");
        try {
            crfChunker= (ChainCrfChunker)AbstractExternalizable.readObject(modelFile);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    public void execute(Tuple tuple) {

        row=tuple.getStringByField("row");
        Chunking chunking = crfChunker.chunk(row);
        Set<String>brandSet=new HashSet<String>();
        Set<String>catSet=new HashSet<String>();
        Map<String,Set<String>> returnMap=new HashMap<String, Set<String>>();
        for(Chunk el:chunking.chunkSet()){
            int start=el.start();
            int end=el.end();
            String chuntText= (String) chunking.charSequence().subSequence(start,end);
            String type=el.type();
            if(type.equals("BND")){
                brandSet.add(chuntText.toLowerCase());
            }
            else if(type.equals("CAT")){
                catSet.add(chuntText.toLowerCase());
            }
        }
        if(brandSet.size()>0){
            returnMap.put("BND",brandSet);

        }
        if (catSet.size()>0){
            returnMap.put("CAT",catSet);
        }
        if(returnMap.size()>0){
            _collector.emit( tuple,new Values(row,returnMap));
        }
        _collector.ack(tuple);




    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("row","returnMap"));

    }







}

