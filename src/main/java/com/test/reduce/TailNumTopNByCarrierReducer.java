package com.test.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @Classname FlightsByCarrierReducer
 * @Description 飞机航班号最多的topN
 * @Date 2020/6/8 11:22
 * @Created by xcp
 */
public class TailNumTopNByCarrierReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static final Logger logger = LoggerFactory.getLogger(TailNumTopNByCarrierReducer.class);

    private int topN;

    private Map<String, Integer> result = new HashMap<>();

    private IntWritable intWritable;
    private Text text;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        topN = configuration.getInt("topN", 1);
        intWritable = new IntWritable();
        text = new Text();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        result.put(key.toString(), sum);
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<Map.Entry<String, Integer>> listData = new ArrayList<>(result.entrySet());
        listData.stream()
                .sorted((o1, o2) -> o2.getValue() - o1.getValue())
                .limit(topN).forEach(siEntry -> {
            intWritable.set(siEntry.getValue());
            text.set(siEntry.getKey());
            try {
                context.write(text, intWritable);
            } catch (IOException | InterruptedException e) {
                logger.error("写入cleanup失败，原因为", e);
            }
        });
    }
}
