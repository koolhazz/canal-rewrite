package com.alibaba.otter.canal.parser;

/**
 * Created by hp on 14-8-19.
 */
public class Configer {

    private String username = null;

    private String password = null;

    private String address = null;

    private long slaveId;

    public long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(long slaveId) {
        this.slaveId = slaveId;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getAddress() {
        return address;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Configer(String usr,String psd,String addr,long slave) {
        this.username = usr;
        this.password = psd;
        this.address = addr;
        this.slaveId = slave;
    }
}
