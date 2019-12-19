package com.virtual.pairprogrammers;

import java.io.Serializable;
import java.util.Date;

public class Character implements Serializable
{
    private Double average;
    private Double low;
    private Double open;
   // private String time;
    private Double high;
    private Double volume;
    private String coinType;
    private String currency;


    public Double getaverage() {
        return average;
    }

    public void setaverage(Double average) {
        this.average = average;
    }


    public Double getlow() {
        return low;
    }

    public void setlow(Double low) {
        this.low = low;
    }

    public Double getopen() {
        return open;
    }

    public void setopen(Double open) {
        this.open = open;
    }

//    public String gettime() {
//        return time;
//    }
//
//    public void settime(String time) {
//        this.time = time;
//    }

    public Double gethigh() {
        return high;
    }

    public void sethigh(Double high) {
        this.high = high;
    }

    public Double getvolume() {
        return volume;
    }

    public void setvolume(Double volume) {
        this.volume = volume;
    }

    public String getcoinType() {
        return coinType;
    }

    public void setcoinType(String coinType) {
        this.coinType = coinType;
    }

    public String getcurrency() {
        return currency;
    }

    public void setcurrency(String currency) {
        this.currency = currency;
    }


}
