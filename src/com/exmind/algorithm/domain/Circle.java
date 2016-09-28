package com.exmind.algorithm.domain;


import java.io.Serializable;

/**
 * Circle类
 *
 * @author dzwl
 * @param center
 *            radius
 * @version 1.0 @2015/09/01 10:15
 */
public class Circle implements Serializable{
    /**
     * 对象序列化
     */
    private static final long serialVersionUID = -5979216570094676239L;
    // 圆心点坐标和半径
    private CircleFencePoint center;
    private float radius;

    public Circle(CircleFencePoint center, float radius) {
        this.center = center;
        this.radius = radius;
    }

    // 获取圆心坐标与半径值
    public CircleFencePoint getCenter() {
        return center;
    }

    public float getRadius() {
        return radius;
    }

    // 设置圆心坐标与半径值
    public void setCenter(CircleFencePoint center) {
        this.center = center;
    }

    public void setRadius(float radius) {
        this.radius = radius;
    }
}
