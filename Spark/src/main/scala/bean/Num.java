package bean;

import java.io.Serializable;

public class Num implements Serializable {
//    private static final long serialVersionUID = 1L;
    public int value;

    public Num(){}

    public Num(int x){this.value = x;}


    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
