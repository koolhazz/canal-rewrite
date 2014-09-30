package com.alibaba.otter.canal.parser;



import com.alibaba.otter.canal.parse.driver.mysql.MysqlConnector;
import com.alibaba.otter.canal.parse.driver.mysql.MysqlQueryExecutor;
import com.alibaba.otter.canal.parse.driver.mysql.MysqlUpdateExecutor;
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.event.QueryLogEvent;
import javafx.geometry.Pos;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by hp on 14-8-19.
 */
public class MysqlConnection {

    private Configer configer =null;

    public MysqlConnection() {
    }

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    private Charset charset = Charset.forName("UTF-8");

    public MysqlConnector getConnector() {
        return connector;
    }

    public void setConnector(MysqlConnector connector) {
        this.connector = connector;
    }

    private MysqlConnector connector = new MysqlConnector();

    public long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(long slaveId) {
        this.slaveId = slaveId;
    }

    private long slaveId ;

    private MysqlQueryExecutor queryExecutor;

    private MysqlUpdateExecutor updateExecutor;

    public MysqlConnection(Configer configer) {
        this.configer = configer;
        this.slaveId=configer.getSlaveId();
        if(configer == null){
            throw new NullPointerException("configer is NULL !!!!");
        }
        String []addr = configer.getAddress().split(":");
        if(addr.length!=2){
            throw new NullPointerException("address format is error!");
        }
        int port = Integer.parseInt(addr[1]);
        connector.setAddress(new InetSocketAddress(addr[0],port));
        connector.setUsername(configer.getUsername());
        connector.setPassword(configer.getPassword());
    }

    public void connect()throws IOException{
        connector.connect();
    }

    public void disconnect()throws IOException{
        connector.disconnect();
    }

    public void reconnect()throws IOException{
        connector.reconnect();
    }

    public boolean isConnected(){
        return(connector.isConnected());
    }

    public ResultSetPacket query(String cmd) throws IOException {
        MysqlQueryExecutor exector = new MysqlQueryExecutor(connector);
        return exector.query(cmd);
    }

    public void update(String cmd) throws IOException {
        MysqlUpdateExecutor exector = new MysqlUpdateExecutor(connector);
        exector.update(cmd);
    }

    public List<CanalEntry.Entry> dump(PositionManager startPos,LogEventConvert eventConvert)throws IOException{
        //update settings
        try {
            update("set wait_timeout=9999999");
            update("set net_write_timeout=1800");
            update("set net_read_timeout=1800");
            update("set names 'binary'");//this will be my try to test no binary
            update("set @master_binlog_checksum= '@@global.binlog_checksum'");
            update("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");
        } catch (Exception e){
            System.out.println("update error!! , "+ e.getMessage());
        }
        //row mode check
        //...
        //send binlog dump
        BinlogDumpCommandPacket binlogDumpCmd = new BinlogDumpCommandPacket();
        binlogDumpCmd.binlogFileName = startPos.getEntryPosition().getJournalName();
        binlogDumpCmd.binlogPosition = startPos.getEntryPosition().getPosition();
        if(configer == null){
            throw new NullPointerException("configer is NULL");
        }
        binlogDumpCmd.slaveServerId = this.configer.getSlaveId();
        byte[] cmdBody = binlogDumpCmd.toBytes();
        HeaderPacket dumpHeader = new HeaderPacket();
        dumpHeader.setPacketBodyLength(cmdBody.length);
        dumpHeader.setPacketSequenceNumber((byte)0x00);
        PacketManager.write(connector.getChannel(),new ByteBuffer[]{ByteBuffer.wrap(dumpHeader.toBytes()),ByteBuffer.wrap(cmdBody)});
        //fetch the event data
        DirectLogFetcher fetcher = new DirectLogFetcher(connector.getReceiveBufferSize());
        fetcher.start(connector.getChannel());
        LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT,LogEvent.ENUM_END_EVENT);
        LogContext context = new LogContext();
        //test
        List<CanalEntry.Entry> entries = new ArrayList<CanalEntry.Entry>();
        //debug
        PositionManager debugPosition = new PositionManager();
        int eventCount = 0;
        while (fetcher.fetch()){
            LogEvent event ;
            event = decoder.decode(fetcher,context);
            if(event == null){
                throw new CanalParseException("parse failed");
            }
            eventCount++;
            //convert the event to entry
            CanalEntry.Entry entry = eventConvert.parse(event);
            System.out.println(">>>>>>>>>>>>>>>>>>>>event: event number-> "+ eventCount + "  " + LogEvent.getTypeName(event.getHeader().getType()) + ", position:" + event.getLogPos());
            if(entry!=null){
                //for Debug
                entries.add(entry);
                MysqlCanalParser.printEntry(entries);
            }
            //write back the position data to file by detecting DML(XID) or DDL(QUERY) or DCL(QUERY)
            if((event.getHeader().getType()==LogEvent.XID_EVENT)
                    ||(event.getHeader().getType()==LogEvent.QUERY_EVENT
                        && !StringUtils.endsWithIgnoreCase(((QueryLogEvent) event).getQuery(),"BEGIN"))){
                PositionManager positionManager = new PositionManager(
                        new EntryPosition(eventConvert.getBinlogFileName()
                                ,event.getLogPos()));
                positionManager.writeDatFile();
            }

            //debug whether xid is end transaction
//            if(event.getHeader().getType() == LogEvent.WRITE_ROWS_EVENT_V1){
//                PositionManager positionManager = new PositionManager(
//                        new EntryPosition(eventConvert.getBinlogFileName()
//                                ,event.getLogPos()));
//                positionManager.writeDatFile();
//            }
            entries.clear();
        }
        do {
            System.out.println("mysql connection is aborted!!!");
            try {
                connector.reconnect();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }while (!connector.isConnected());
        return (entries);
    }

    public MysqlConnection fork() {
        MysqlConnection connection = new MysqlConnection();
        connection.setCharset(getCharset());
        connection.setSlaveId(getSlaveId());
        connection.setConnector(connector.fork());
        return connection;
    }

}
