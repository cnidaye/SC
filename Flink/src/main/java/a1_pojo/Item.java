package a1_pojo;

public class Item{
    public String name ;
    public Integer score;
    public Long checkTime;

    public Item(){}
    public Item(String name, Integer score) {
        this.name = name;
        this.score = score;
    }

    public Item(String name, Integer score, Long checkTime) {
        this.name = name;
        this.score = score;
        this.checkTime = checkTime;
    }

    @Override
    public String toString() {
        return "Item{" +
                "name='" + name + '\'' +
                ", score=" + score +
                '}';
    }

}