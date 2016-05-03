package fr.pomverte;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import example.avro.User;

public class BinaryCodingTest {

    @Test
    public void createSerializeDeserialize() throws IOException {
        // CREATING USERS
        User user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(256);
        // Leave favorite color null

        // Alternate constructor
        User user2 = new User("Ben", 7, "red");

        // Construct via builder
        User user3 = User.newBuilder().setName("Charlie").setFavoriteColor("blue").setFavoriteNumber(null).build();

        // SERIALIZING
        // Serialize user1, user2 and user3 to disk
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        userDatumWriter.write(user1, encoder);
        userDatumWriter.write(user2, encoder);
        userDatumWriter.write(user3, encoder);
        // flush the binary code into the outputstream
        encoder.flush();

        // DESERIALIZE USERS FROM binary
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);

        int counter = 0;
        try {
            while (true) {
                User userDeserialized = new User();
                userDatumReader.read(userDeserialized, decoder);
                System.out.println(userDeserialized);
                counter++;
            }
        } catch (EOFException e) {

        }
        assertEquals("3 Users have been created", 3, counter);
    }

}
