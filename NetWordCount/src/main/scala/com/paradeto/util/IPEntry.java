package com.paradeto.util;

/**
 * Created by Ayou on 2015/11/9.
 */
public class IPEntry {
    public String beginIp;
    public String endIp;
    public String country;
    public String area;

    /**
     * 构造函数
     */



    public IPEntry() {
        beginIp = endIp = country = area = "";
    }

    public String toString(){
        return this.area+"  "+this.country+"IP范围:"+this.beginIp+"-"+this.endIp;
    }
}
