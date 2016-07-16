package fr.pomverte;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

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

    public static List<GenericRecord> createRecord(Schema schema) {
        // CREATE RECORD
        GenericRecord record1 = new GenericData.Record(schema);
        record1.put("name", "Alyssa");
        record1.put("favorite_number", 256);
        // Leave favorite color null

        GenericRecord record2 = new GenericData.Record(schema);
        record2.put("name", "Ben");
        record2.put("favorite_number", 7);
        record2.put("favorite_color", "red");

        ArrayList<GenericRecord> result = new ArrayList<>();
        result.add(record1);
        result.add(record2);
        return result;
    }
}
