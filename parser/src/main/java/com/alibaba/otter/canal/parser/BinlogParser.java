package com.alibaba.otter.canal.parser;

/**
 * Created by hp on 14-8-19.
 */

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * 解析binlog的接口
 *
 * @author: yuanzu Date: 12-9-20 Time: 下午8:46
 */
public interface BinlogParser<T> extends CanalLifeCycle {

    CanalEntry.Entry parse(T event) throws CanalParseException;

    void reset();
}
