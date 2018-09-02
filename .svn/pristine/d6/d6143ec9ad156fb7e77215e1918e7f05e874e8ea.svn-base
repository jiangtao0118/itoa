package com.wisdom.spark.etl.ppn;

import java.io.Serializable;

public class DFT implements Serializable{
    //计算频域的一项
    public static com.wisdom.spark.etl.ppn.Complex dft_k(double[] x, int k) {
        int N = x.length;
        double real = 0;
        double imag = 0;
        for(int n=0; n<N; n++)
        {
            double th = -2*Math.PI*k*n/N;
            real += Math.cos(th)*x[n];
            imag += Math.sin(th)*x[n];
        }

        return new com.wisdom.spark.etl.ppn.Complex(real,imag);
    }

    //根据时域计算频域
    public static Complex[] dft(double[] x) {
        int N = x.length;
        Complex[] y = new Complex[N];
        for(int k=0; k<N; k++) {
            y[k] = dft_k(x,k);
        }
        return y;
    }

    /**
     * 根据时域计算频域
     * @param x 表示Idle_CPU一列数据
     * @return
     */
    public static Complex[] dft(Complex[] x) {
        int N = x.length;
        double[] xOfDouble = new double[N];
        for(int i=0;i<N;i++)
        {
            xOfDouble[i] = x[i].re();
        }
        return dft(xOfDouble);
    }

    /**
     * 计算频域的一项
     * @param y 表示已经生成的预测模型
     * @param n 表示要预测的点
     * @return
     */
    public static Complex idft_n(Complex[] y,int n) {
        int N = y.length;
        double real = 0;
        double imag = 0;
        for(int k=0; k<N; k++)
        {
            if(y[k].re()==0 && y[k].im()==0) continue;

            double th = 2*Math.PI*k*n/N;

            real += y[k].re()*Math.cos(th) - y[k].im()*Math.sin(th);
            imag += y[k].im()*Math.cos(th) + y[k].re()*Math.sin(th);
        }

        return new Complex(real/N,imag/N);
    }

    //根据频域计算时域
    public static Complex[] idft(Complex[] y) {
        int N = y.length;
        Complex[] x = new Complex[N];
        for(int n=0; n<N; n++) {
            x[n] = idft_n(y,n);
        }
        return x;
    }

}
