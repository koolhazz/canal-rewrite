package com.alibaba.otter.canal.parser;


import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by hp on 14-8-19.
 */
public class MysqlCanalParser extends CanalParser {

    protected final static Logger logger             = LoggerFactory.getLogger(MysqlCanalParser.class);

    private Configer configer ;

    private MysqlConnection mysqlconn ;

    private MysqlConnection metaConn;//get the colunm info

    private TableMetaCache tableMetaCache;

    private PositionManager startPosition ;

    private boolean running = true;

    List<MysqlLogData> logDatas = new ArrayList<MysqlLogData>();

    private LogEventConvert eventConvert;

    public void simpleGet(){
       //while(running){
            try {
                //pre configer and connector
                configer = new Configer("canal","canal","127.0.0.1:3306",1234);//username,password,address,slaveId
                mysqlconn = new MysqlConnection(configer);
                //pre dump
                eventConvert = new LogEventConvert();
                metaConn = mysqlconn.fork();
                try {
                    metaConn.connect();
                } catch (IOException e) {
                    throw new CanalParseException("meta connect failed!");
                }
                tableMetaCache = new TableMetaCache(metaConn);
                eventConvert.setTableMetaCache(tableMetaCache);
                //connect
                mysqlconn.connect();
                startPosition = findStartPosition(mysqlconn);
                if(startPosition == null){
                    throw new NullPointerException("start position is null");
                }
                logger.info("dump the start position");
                List<CanalEntry.Entry> entries = mysqlconn.dump(startPosition,eventConvert);
                logger.info("get the dump successfully!");
                System.out.println("dump complete!!");
                for(CanalEntry.Entry entry : entries){
                    MysqlLogData logData = new MysqlLogData(entry);
                    logDatas.add(logData);
                }
                //after dump
                if(metaConn!=null){
                    try {
                        metaConn.disconnect();
                    }catch (IOException e){
                        throw new CanalParseException("meta disconnect failed!");
                    }
                }
                //printEntry(entries);
            } catch (Exception e){
                e.printStackTrace();
            }
       //}
    }

    private PositionManager findStartPosition(MysqlConnection conn) throws IOException{
        //load position from file
        PositionManager positionManager = findFileStartPostion();
        if(positionManager==null){
            positionManager = findMysqlStartPosition(conn);
        }
        return(positionManager);
    }

    private PositionManager findFileStartPostion() throws IOException{
        PositionManager positionManager = new PositionManager();
        if(positionManager.readDatFile()){
            if(positionManager.getEntryPosition().getJournalName()!=null
                    &&!positionManager.getEntryPosition().getJournalName().equals("")
                    &&positionManager.getEntryPosition().getPosition()!=0){
                return(positionManager);
            }
            else{
                return(null);
            }
        }
        else{
            return(null);
        }
    }

    private PositionManager findMysqlStartPosition(MysqlConnection conn){

        try {
            ResultSetPacket packet = conn.query("show master status");
            List<String> fields = packet.getFieldValues();
            if (CollectionUtils.isEmpty(fields)) {
                throw new CanalParseException("command : 'show master status' has an error! pls check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation");
            }
            EntryPosition endPosition = new EntryPosition(fields.get(0),Long.valueOf(fields.get(1)));
            return(new PositionManager(endPosition));
        }catch (IOException e){
            throw new CanalParseException("command: 'show master status' has an error! ",e);
        }

    }

    public static void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                    CanalEntry.TransactionBegin begin = null;
                    try {
                        begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    System.out.println("============================================================================> TRANSACTIONBEGIN EVENT :");
                    System.out.println("binlog name:"+entry.getHeader().getLogfileName());
                    System.out.println("log file offset:"+String.valueOf(entry.getHeader().getLogfileOffset()));
                    System.out.println("execute time:"+String.valueOf(entry.getHeader().getExecuteTime()));
                    System.out.println("delay time:"+String.valueOf(delayTime));
                    System.out.println("BEGIN ----> Thread id:"+begin.getThreadId());
                } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                    CanalEntry.TransactionEnd end = null;
                    try {
                        end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    System.out.println("============================================================================> TRANSACTIONEND EVENT :");
                    System.out.println("binlog name:"+entry.getHeader().getLogfileName());
                    System.out.println("log file offset:"+String.valueOf(entry.getHeader().getLogfileOffset()));
                    System.out.println("execute time:"+String.valueOf(entry.getHeader().getExecuteTime()));
                    System.out.println("delay time:"+String.valueOf(delayTime));
                    System.out.println("END ----> transaction id:"+end.getTransactionId());
                }

                continue;
            }

            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChage = null;
                try {
                    rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                CanalEntry.EventType eventType = rowChage.getEventType();
                //print the row information
                System.out.println("============================================================================> ROWDATA EVENT :");
                System.out.println("binlog name:"+entry.getHeader().getLogfileName());
                System.out.println("log file offset:"+String.valueOf(entry.getHeader().getLogfileOffset()));
                System.out.println("schema name:"+entry.getHeader().getSchemaName());
                System.out.println("table name:"+entry.getHeader().getTableName());
                System.out.println("event type:"+eventType);
                System.out.println("execute time:"+String.valueOf(entry.getHeader().getExecuteTime()));
                System.out.println("delay time:"+String.valueOf(delayTime));

                if (rowChage.getIsDdl()) {
                    System.out.println("SQL ----> " + rowChage.getSql());
                    //continue;
                }

                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType == CanalEntry.EventType.DELETE) {
                        printColumn(rowData.getBeforeColumnsList());
                    } else if (eventType == CanalEntry.EventType.INSERT) {
                        printColumn(rowData.getAfterColumnsList());
                    } else {//update
                        //before
                        printColumn(rowData.getBeforeColumnsList());
                        //after
                        printColumn(rowData.getAfterColumnsList());
                        //updated
                        printColumn(rowData.getBeforeColumnsList(),rowData.getAfterColumnsList());
                    }
                }
            }
        }
    }


    public static void printColumn(List<CanalEntry.Column> columns) {
        //print the column information
        System.out.println("------------------------------------------->column info :");
        for (CanalEntry.Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName() + " : " + column.getValue());
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }
            System.out.println(builder);
        }
    }

    public static void printColumn(List<CanalEntry.Column> columns1,List<CanalEntry.Column> columns2) {
        //print the column information
        System.out.println("------------------------------------------->updated column info :");
        for(int i=0;i<=columns2.size()-1;i++){
            StringBuilder builder = new StringBuilder();
            if(columns2.get(i).getIsKey()||columns2.get(i).getUpdated()){
                builder.append(columns2.get(i).getName() + " : " + columns2.get(i).getValue());
                builder.append("    type=" + columns2.get(i).getMysqlType());
                System.out.println(builder);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        MysqlCanalParser mysqlCanalParser = new MysqlCanalParser();
        mysqlCanalParser.simpleGet();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                System.out.println("test for addShutdownHook");
            }

        });
    }
}
