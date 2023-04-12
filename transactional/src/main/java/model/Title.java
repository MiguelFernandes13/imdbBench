package model;

public class Title {
    public String id;
    public String name;
    public String type;

    @Override
    public String toString() {
        return "Title{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
