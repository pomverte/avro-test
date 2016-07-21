package fr.pomverte;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import example.avro.User;

/**
 * {@link https://avro.apache.org/docs/current/gettingstartedjava.html}
 *
 */
public class CodeGenerationTest {

    private static final String JSON_USER_ARRAY = "[{\"name\":\"Alyssa\",\"favorite_number\":{\"int\":256},\"favorite_color\":null}, {\"name\":\"Ben\",\"favorite_number\":{\"int\":7},\"favorite_color\":{\"string\":\"red\"}}, {\"name\":\"Charlie\",\"favorite_number\":null,\"favorite_color\":{\"string\":\"blue\"}}]";

    @Test
    public void fileSerializationTest() throws IOException {
        // SERIALIZING
        // Serialize user1, user2 and user3 to disk
        File outputFile = WriterHelper.writeRecordToFile(User.class, DataHelper.createUser(),
                new File("target/users.avro"));

        // DESERIALIZE USERS FROM DISK
        List<User> readFromFile = ReaderHelper.readSpecificFromFile(User.class, outputFile);
        assertEquals("3 Users have been created", 3, readFromFile.size());
    }

    @Test
    public void streamSerializationTest() throws IOException {
        // SERIALIZING
        // Serialize user1, user2 and user3 into a stream
        ByteArrayOutputStream outputStream = WriterHelper.writeRecordToStream(User.class, DataHelper.createUser());

        // DESERIALIZE USERS FROM binary
        List<User> users = ReaderHelper.readSpecificFromStream(User.class, outputStream);
        assertEquals("3 Users have been created", 3, users.size());
    }

    @Test
    public void jsonSerializationTest() throws IOException {
        // SERIALIZING
        // Serialize user1, user2 and user3 into a JSON array
         String json = WriterHelper.writeRecordToJson(User.getClassSchema(), User.class, false,
         DataHelper.createUser());
         assertEquals(JSON_USER_ARRAY, json);
    }

}
