package com.exmind.algorithm.function;

import com.exmind.algorithm.domain.Circle;
import com.exmind.algorithm.domain.CircleFencePoint;

/**
 * @CircleFence类 * 判断点是否在圆范围内（圆内包括在弧上）
 * @param point
 *            被判定点
 * @param circle
 *            圆形区域
 * @author dzwl
 * @version 1.0 @2015/08/19 14：26
 */
public class CircleFence {

    public boolean isIn(CircleFencePoint point, Circle circle) {
        if (getDistance(point, circle.getCenter()) <= circle.getRadius()) {
            return true;
        }
        return false;
    }

    // 获取点到圆心距离
    public float getDistance(CircleFencePoint point, CircleFencePoint center) {
        return (float) Math.sqrt((point.getX() - center.getX()) * (point.getX() - center.getX())
                + (point.getY() - center.getY()) * (point.getY() - center.getY()));
    }
}
