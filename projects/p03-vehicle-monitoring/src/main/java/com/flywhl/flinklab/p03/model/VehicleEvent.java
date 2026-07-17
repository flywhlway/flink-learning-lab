package com.flywhl.flinklab.p03.model;

/**
 * 车联网遥测事件（p03 CEP 输入 POJO）。
 *
 * <p>字段对齐 RESEARCH：vin / signalType / value / eventTime。
 */
public final class VehicleEvent {

    public String vin;
    public String signalType;
    public double value;
    public long eventTime;

    public VehicleEvent() {
    }

    public VehicleEvent(String vin, String signalType, double value, long eventTime) {
        this.vin = vin;
        this.signalType = signalType;
        this.value = value;
        this.eventTime = eventTime;
    }
}
