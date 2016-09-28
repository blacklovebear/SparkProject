package com.exmind.algorithm.domain;

import java.io.Serializable;

public class CircleFencePoint implements Serializable {

    /**
     * 对象序列化
     */
    private static final long serialVersionUID = -4832382680451995608L;
    // 点坐标x, y
    private float x;
    private float y;

    public CircleFencePoint(float x, float y) {
        this.x = x;
        this.y = y;
    }

    public CircleFencePoint(String x, String y) {
        this.x = Float.parseFloat(x);
        this.y = Float.parseFloat(y);
    }

    public CircleFencePoint() {

    }

    public float getX() {
        return x;
    }

    public void setX(float x) {
        this.x = x;
    }

    public float getY() {
        return y;
    }

    public void setY(float y) {
        this.y = y;
    }

}
