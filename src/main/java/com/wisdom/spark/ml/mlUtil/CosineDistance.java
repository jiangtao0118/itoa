package com.wisdom.spark.ml.mlUtil;

import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.apache.commons.math3.stat.descriptive.summary.SumOfSquares;
import org.apache.commons.math3.util.MathArrays;
import org.apache.commons.math3.util.MathUtils;

/**
 * Created by tup on 2017/1/25.
 */
public class CosineDistance implements DistanceMeasure {

    public double compute(double[] a, double[] b) {

        double distance = 1.0;
        double anorm = MathArrays.safeNorm(a);
        double bnorm = MathArrays.safeNorm(b);

        //若a或b的2范数是0，则其是坐标原点，为避免NaN直接返回1
        if(Math.abs(anorm * bnorm - 0.0) < 1E-7){
            return distance;
        }

        Sum sum = new Sum();
        double[] multiply = MathArrays.ebeMultiply(a,b);
        double dotProduct = sum.evaluate(multiply,0,multiply.length);

        distance = 1 - dotProduct/(anorm*bnorm);


//        System.out.println(distance);

        return distance;

    }

}
