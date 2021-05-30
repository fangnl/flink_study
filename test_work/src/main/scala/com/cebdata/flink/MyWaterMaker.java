package com.cebdata.flink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import scala.Tuple2;

public class MyWaterMaker implements WatermarkGenerator<Tuple2<String, Long>> {

    long per = 2000L;
    long maxTimestamp = 0L;


    /**
     * 每条数据来时调用一次
     *
     * @param event
     * @param eventTimestamp
     * @param output
     */
    @Override
    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {

        maxTimestamp = Math.max(event._2, maxTimestamp);
    }

    /**
     * 每间隔一定的时间调用一次该方法 默认 200毫秒
     *
     * @param output
     */

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {


        output.emitWatermark(new Watermark(maxTimestamp - per));

    }
}
