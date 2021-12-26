package a1_pojo;

public class TaxiOrder {
    public String orderId;
    public String driverId;
    public String passengerId;
    public Long startTime;
    public Long endTime;

    public TaxiOrder() {
    }

    public TaxiOrder(String orderId, Long startTime, Long endTime) {
        this.orderId = orderId;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public TaxiOrder(String orderId, Long startTime) {
        this.orderId = orderId;
        this.startTime = startTime;
    }

}
