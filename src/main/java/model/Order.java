//package model;
//
//import lombok.Data;
//
//import java.sql.Timestamp;
//
///**
// * @author ：why
// * @description：TODO
// * @date ：2020/10/26 22:08
// */
//
//@Data
//public class Order implements java.io.Serializable{
//    private String id;
//    private String commodity_id;
//    private String order_num;
//    private String usercode;
//    private String status;
//    private String amount;
//    private String phone_num;
//    private Timestamp create_time;
//    private String addr;
//    private String order_flag;
//
//    public Order(String id, String commodity_id, String order_num, String usercode, String status, String amount, String phone_num, Timestamp create_time, String addr, String order_flag) {
//        this.id = id;
//        this.commodity_id = commodity_id;
//        this.order_num = order_num;
//        this.usercode = usercode;
//        this.status = status;
//        this.amount = amount;
//        this.phone_num = phone_num;
//        this.create_time = create_time;
//        this.addr = addr;
//        this.order_flag = order_flag;
//    }
//
//    public String getId() {
//        return id;
//    }
//
//    public void setId(String id) {
//        this.id = id;
//    }
//
//    public String getCommodity_id() {
//        return commodity_id;
//    }
//
//    public void setCommodity_id(String commodity_id) {
//        this.commodity_id = commodity_id;
//    }
//
//    public String getOrder_num() {
//        return order_num;
//    }
//
//    public void setOrder_num(String order_num) {
//        this.order_num = order_num;
//    }
//
//    public String getUsercode() {
//        return usercode;
//    }
//
//    public void setUsercode(String usercode) {
//        this.usercode = usercode;
//    }
//
//    public String getStatus() {
//        return status;
//    }
//
//    public void setStatus(String status) {
//        this.status = status;
//    }
//
//    public String getAmount() {
//        return amount;
//    }
//
//    public void setAmount(String amount) {
//        this.amount = amount;
//    }
//
//    public String getPhone_num() {
//        return phone_num;
//    }
//
//    public void setPhone_num(String phone_num) {
//        this.phone_num = phone_num;
//    }
//
//    public Timestamp getCreate_time() {
//        return create_time;
//    }
//
//    public void setCreate_time(Timestamp create_time) {
//        this.create_time = create_time;
//    }
//
//    public String getAddr() {
//        return addr;
//    }
//
//    public void setAddr(String addr) {
//        this.addr = addr;
//    }
//
//    public String getOrder_flag() {
//        return order_flag;
//    }
//
//    public void setOrder_flag(String order_flag) {
//        this.order_flag = order_flag;
//    }
//}
