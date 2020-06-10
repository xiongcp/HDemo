package com.test.mapper;

import au.com.bytecode.opencsv.CSVParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Classname FlightsByCarrierMapper
 * @Description 飞机编码去重
 * @Date 2020/6/8 11:21
 * @Created by xcp
 */
public class TailNumByCarrierMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static final Logger logger = LoggerFactory.getLogger(TailNumByCarrierMapper.class);

    private CSVParser csvParser;

    public TailNumByCarrierMapper() {
        csvParser = new CSVParser();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (StringUtils.isNotBlank(value.toString())) {
            String[] line = csvParser.parseLine(value.toString());
            if (line.length < 30) {
                return;
            }
            String tailNum = line[11];
            context.write(new Text(tailNum), NullWritable.get());
        }
    }
}
