package cn.sky.database.data;

import lombok.Data;
import lombok.ToString;

import java.util.Date;

/**
 * @author Sky
 * @date 2018/4/18 下午12:43
 */
@Data
@ToString
public class BusinessData {

    private int id;

    private String bnName;              //max length is 200B

    private String status;              //max length is 12B

    private Date registerDate;          //max length is 10B

    private Date cancelDate;            //max length is 10B

    private Date renewDate;             //max length is 10B

    private String stateNum;            //max length is 10B

    private String stateOfReg;          //max length is 3B

    private String abn;                 //max length is 20B


}
