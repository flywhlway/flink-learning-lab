package com.flywhl.flinklab.e12;

/** 输入事件 POJO:车辆信号(与 ai/chapters/07 示例一致)。 */
public class VehicleSignal {
    public String vin;
    public String type;
    public double value;
    public double threshold;

    public VehicleSignal() {
    }

    public VehicleSignal(String vin, String type, double value, double threshold) {
        this.vin = vin;
        this.type = type;
        this.value = value;
        this.threshold = threshold;
    }

    @Override
    public String toString() {
        return "VehicleSignal{vin=%s,type=%s,value=%.1f,threshold=%.1f}"
                .formatted(vin, type, value, threshold);
    }
}
