package com.alibaba.otter.canal.parser;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * Created by hp on 14-8-19.
 */
public class MysqlLogData extends LogData {

    CanalEntry.Entry entry;

    public MysqlLogData(CanalEntry.Entry entry) {
        this.entry = entry;
    }

    //other data field
}
