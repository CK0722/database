package cn.sky.database.constant;

/**
 * data structure describing valid columns in the dataset
 *
 * @author Sky
 * @date 2018/4/18 下午2:30
 */
public enum BusinessEnum {
    BN_NAME(1, "BN_NAME"),
    BN_STATUS(2, "BN_STATUS"),
    BN_REG_DT(3, "BN_REG_DT"),
    BN_CANCEL_DT(4, "BN_CANCEL_DT"),
    BN_RENEW_DT(5, "BN_RENEW_DT"),
    BN_STATE_NUM(6, "BN_STATE_NUM"),
    BN_STATE_OF_REG(7, "BN_STATE_OF_REG"),
    BN_ABN(8, "BN_ABN");

    private int index;

    private String field;

    BusinessEnum(int index, String field) {
        this.index = index;
        this.field = field;
    }

    public int getIndex() {
        return index;
    }

    public String getField() {
        return field;
    }

    public static String getFieldByIndex(int index) {
        switch (index) {
            case 1:
                return BusinessEnum.BN_NAME.getField();
            case 2:
                return BusinessEnum.BN_STATUS.getField();
            case 3:
                return BusinessEnum.BN_REG_DT.getField();
            case 4:
                return BusinessEnum.BN_CANCEL_DT.getField();
            case 5:
                return BusinessEnum.BN_RENEW_DT.getField();
            case 6:
                return BusinessEnum.BN_STATE_NUM.getField();
            case 7:
                return BusinessEnum.BN_STATE_OF_REG.getField();
            case 8:
                return BusinessEnum.BN_ABN.getField();
            default:
                return "unknown";
        }
    }

}
