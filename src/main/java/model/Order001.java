package model;

import lombok.Data;

/**
 * @author ：why
 * @description：TODO
 * @date ：2020/10/26 22:08
 */

@Data
public class Order001 {
    private String id;
    private String bb;

    public Order001(String id, String bb) {
        this.id = id;
        this.bb = bb;
    }
}
