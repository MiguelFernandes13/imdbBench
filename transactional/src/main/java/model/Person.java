package model;

import java.util.List;

public class Person {
    public String name;
    public String job;
    public List<String> characters;

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", job='" + job + '\'' +
                ", characters=" + characters +
                '}';
    }
}
