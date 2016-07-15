package fr.pomverte;

import java.util.ArrayList;
import java.util.List;

import example.avro.User;

public class DataHelper {

    public static List<User> createUser() {
        // CREATING USERS
        User user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(256);
        // Leave favorite color null

        // Alternate constructor
        User user2 = new User("Ben", 7, "red");

        // Construct via builder
        User user3 = User.newBuilder().setName("Charlie").setFavoriteColor("blue").setFavoriteNumber(null).build();

        ArrayList<User> result = new ArrayList<>();
        result.add(user1);
        result.add(user2);
        result.add(user3);
        return result;
    }

}
