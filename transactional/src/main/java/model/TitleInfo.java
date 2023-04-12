package model;

import java.util.Map;

public class TitleInfo {
    public String titleName;
    public String titleType;
    public int runtime;
    public int startYear;
    public Map<String, Person> castAndCrew;
    public String titleUserRegion;

    @Override
    public String toString() {
        return "TitleInfo{" +
                "titleName='" + titleName + '\'' +
                ", titleType='" + titleType + '\'' +
                ", runtime=" + runtime +
                ", startYear=" + startYear +
                ", castAndCrew=" + castAndCrew +
                ", titleUserRegion='" + titleUserRegion + '\'' +
                '}';
    }
}
