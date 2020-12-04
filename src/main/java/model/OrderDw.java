package model;

import lombok.Data;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/26 22:08
 */

@Data
public class OrderDw {
    private String id;
    private String commodity_id;
    private String order_num;
    private String usercode;
    private String status;
    private String amount;
    private String phone_num;
    private String create_time;
    private String addr;
    private String order_flag;
    private String user_name;
    private String user_id;
    private String phone;
    private String email;
    private String grade;
    private String commodity_name;
    private String commodity_colour;
    private String commodity_no;
    private String description;
}
