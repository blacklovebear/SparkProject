package com.exmind.algorithm.domain;

import java.io.Serializable;

public class ElectronicFenceLineSegment implements Serializable{

    /**
     * 对象序列化
     */
    private static final long serialVersionUID = 8182072065326326448L;
    public ElectronicFencePoint pt1;
    public ElectronicFencePoint pt2;

    public ElectronicFenceLineSegment() {
        this.pt1 = new ElectronicFencePoint();
        this.pt2 = new ElectronicFencePoint();
    }
}
