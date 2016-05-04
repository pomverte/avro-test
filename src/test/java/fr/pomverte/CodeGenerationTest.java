package fr.pomverte;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
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

/**
 * {@link https://avro.apache.org/docs/current/gettingstartedjava.html}
 *
 */
public class CodeGenerationTest {

    private List<User> createUser() {
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

    @Test
    public void fileSerializationTest() throws IOException {
        List<User> users = this.createUser();

        // SERIALIZING
        // Serialize user1, user2 and user3 to disk
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        File outputFile = new File("target/users.avro");
        User user1 = users.get(0);
        dataFileWriter.create(user1.getSchema(), outputFile);
        dataFileWriter.append(user1);
        dataFileWriter.append(users.get(1));
        dataFileWriter.append(users.get(2));
        dataFileWriter.close();

        // DESERIALIZE USERS FROM DISK
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        @SuppressWarnings("resource")
        DataFileReader<User> dataFileReader = new DataFileReader<User>(outputFile, userDatumReader);
        User user = null;
        int counter = 0;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            user = dataFileReader.next(user);
            System.out.println(user);
            counter++;
        }
        assertEquals("3 Users have been created", 3, counter);
    }

    @Test
    public void streamSerializationTest() throws IOException {
        List<User> users = this.createUser();

        // SERIALIZING
        // Serialize user1, user2 and user3 to disk
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        userDatumWriter.write(users.get(0), encoder);
        userDatumWriter.write(users.get(1), encoder);
        userDatumWriter.write(users.get(2), encoder);
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
