package com.exmind.algorithm.function;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.exmind.algorithm.domain.ElectronicFencePoint;
import com.exmind.algorithm.domain.ElectronicFenceLineSegment;

/**
 * ElectronicFence类 判断一个点是否在矩形范围内
 *
 * @param point,
 *            被判断点
 * @param rectangle,
 *            给出区域矩阵的四个点
 * @author dzwl
 * @version 1.0 @2015/08/19 14：26
 */
public class ElectronicFence {

    BigDecimal INFINITY = new BigDecimal(1e10);
    int MAX_N = 1000;
    BigDecimal zero = new BigDecimal(0);
    // 允许的偏差
    BigDecimal ESP = new BigDecimal(1e-9);
    ArrayList<ElectronicFencePoint> Polygon;

    // 计算叉乘 |P0P1| × |P0P2|
    BigDecimal Multiply(ElectronicFencePoint p1, ElectronicFencePoint p2, ElectronicFencePoint p0) {
        return p1.x.subtract(p0.x).multiply(p2.y.subtract(p0.y)).subtract(p2.x.subtract(p0.x).multiply(p1.y.subtract(p0.y)));
    }

    // 判断线段是否包含点point
    private boolean IsOnline(ElectronicFencePoint point, ElectronicFenceLineSegment line) {
        return (Multiply(line.pt1, line.pt2, point).abs().compareTo(ESP) < 0)
                && (point.x.subtract(line.pt1.x).multiply(point.x.subtract(line.pt2.x)).compareTo(zero) <= 0)
                && (point.y.subtract(line.pt1.y).multiply(point.y.subtract(line.pt2.y)).compareTo(zero) <= 0);
    }

    // 判断线段相交
    private boolean Intersect(ElectronicFenceLineSegment L1, ElectronicFenceLineSegment L2) {
        return (L1.pt1.x.max(L1.pt2.x).compareTo(L2.pt1.x.min(L2.pt2.x)) >= 0)
                && (L2.pt1.x.max(L2.pt2.x).compareTo(L1.pt1.x.min(L1.pt2.x)) >= 0)
                && (L1.pt1.y.max(L1.pt2.y).compareTo(L2.pt1.y.min(L2.pt2.y)) >= 0)
                && (L2.pt1.y.max(L2.pt2.y).compareTo(L1.pt1.y.min(L1.pt2.y)) >= 0)
                && (Multiply(L2.pt1, L1.pt2, L1.pt1).multiply(Multiply(L1.pt2, L2.pt2, L1.pt1)).compareTo(zero) >= 0)
                && (Multiply(L1.pt1, L2.pt2, L2.pt1).multiply(Multiply(L2.pt2, L1.pt2, L2.pt1)).compareTo(zero) >= 0);
    }

	/*
	 * 射线法判断点q与多边形polygon的位置关系，要求polygon为简单多边形，顶点逆时针/顺时针排列
	 * 如果点在多边形内： 返回0
	 * 如果点在多边形边上：返回0
	 * 如果点在多边形外： 返回1
	 */

    public int InPolygon(List<ElectronicFencePoint> polygon, ElectronicFencePoint point) {
        int n = polygon.size();
        int count = 0;
        ElectronicFenceLineSegment line = new ElectronicFenceLineSegment();
        line.pt1 = point;
        line.pt2.y = point.y;
        line.pt2.x = INFINITY.negate();

        for (int i = 0; i < n; i++) {
            // 得到多边形的一条边
            ElectronicFenceLineSegment side = new ElectronicFenceLineSegment();
            side.pt1 = polygon.get(i);
            side.pt2 = polygon.get((i + 1) % n);

            if (IsOnline(point, side)) {
                return 0;
            }

            // 如果side平行x轴则不作考虑
            if (side.pt1.y.subtract(side.pt2.y).abs().compareTo(ESP) < 0) {
                continue;
            }

            if (IsOnline(side.pt1, line)) {
                if (side.pt1.y.compareTo(side.pt2.y) > 0)
                    count++;

            } else if (IsOnline(side.pt2, line)) {
                if (side.pt2.y.compareTo(side.pt1.y) > 0)
                    count++;

            } else if (Intersect(line, side)) {
                count++;
            }
        }

        if (count % 2 == 1)
            return 0;
        else
            return 1;
    }

    public static void main(String[] args) {
        ElectronicFence systemTaskJob = new ElectronicFence();
        ArrayList<ElectronicFencePoint> polygon = new ArrayList<ElectronicFencePoint>();
        String pointStr = args[0];
        String[] points = pointStr.split(";");
        for (int i = 0; i < points.length; i++) {
            ElectronicFencePoint point = new ElectronicFencePoint(points[i].split(",")[0], points[i].split(",")[1]);
            polygon.add(point);
        }
        ElectronicFencePoint checkPoint = new ElectronicFencePoint(args[1].split(",")[0], args[1].split(",")[1]);
        int tmp = systemTaskJob.InPolygon(polygon, checkPoint);
        System.out.println("=========" + tmp);
    }
}
