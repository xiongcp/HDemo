package com.test.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Classname FlightsByCarrierReducer
 * @Description 飞机编码去重
 * @Date 2020/6/8 11:22
 * @Created by xcp
 */
public class TailNumByCarrierReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    private static final Logger logger = LoggerFactory.getLogger(TailNumByCarrierReducer.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
