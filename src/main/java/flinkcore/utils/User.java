package flinkcore.utils;

import lombok.Data;

@Data
public class User {


    public User(String name, int age, long createTime) {
        this.name = name;
        this.age = age;
        this.createTime = createTime;
    }

    String name;
    int age;
    long createTime;
}
