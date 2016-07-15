///////////////////////////////////////////////////////////////////////////////
//
// Ce fichier fait partie du projet Synergie - (C) Copyright 2014
// Tous droits réservés à L'Agence de Services et de Paiement (ASP).
//
// Tout ou partie de Synergie ne peut être copié et/ou distribué
// sans l'accord formel de l'ASP.
//
///////////////////////////////////////////////////////////////////////////////
package fr.pomverte.helper;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import example.avro.User;

public final class UserDataSetHelper {

    private UserDataSetHelper() {
    }

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

        ArrayList<User> result = new ArrayList<User>();
        result.add(user1);
        result.add(user2);
        result.add(user3);
        return result;
    }

    public static List<GenericRecord> createRecord(Schema schema) {
        // CREATE USER
        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);
        // Leave favorite color null

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");
        
        ArrayList<GenericRecord> result = new ArrayList<GenericRecord>();
        result.add(user1);
        result.add(user2);
        return result;
    }
}
