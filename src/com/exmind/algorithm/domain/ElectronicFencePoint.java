package com.exmind.algorithm.domain;

import com.exmind.algorithm.function.ElectronicFence;

import java.io.Serializable;
import java.math.BigDecimal;

public class ElectronicFencePoint implements Serializable {
    /**
     * 对象序列化
     */
    private static final long serialVersionUID = -6597892986174527545L;
    // 经度
    public BigDecimal x;
    // 纬度
    public BigDecimal y;

    public ElectronicFencePoint() {

    }

    public ElectronicFencePoint(float x, float y) {
        this.x = new BigDecimal(x);
        this.y = new BigDecimal(y);
    }

    public ElectronicFencePoint(String x, String y) {
        this.x = new BigDecimal(x);
        this.y = new BigDecimal(y);
    }

    public BigDecimal getX() {
        return x;
    }

    public void setX(BigDecimal x) {
        this.x = x;
    }

    public BigDecimal getY() {
        return y;
    }

    public void setY(BigDecimal y) {
        this.y = y;
    }

}
