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

    @Test
    public void fileSerializationTest() throws IOException {
        // SERIALIZING
        // Serialize user1, user2 and user3 to disk
        File outputFile = WriterHelper.writeRecordToFile(User.class, DataHelper.createUser(),
                new File("target/users.avro"));

        // DESERIALIZE USERS FROM DISK
        List<User> readFromFile = ReaderHelper.readFromFile(User.class, outputFile);
        assertEquals("3 Users have been created", 3, readFromFile.size());
    }

    @Test
    public void streamSerializationTest() throws IOException {
        // SERIALIZING
        // Serialize user1, user2 and user3 into a stream
        ByteArrayOutputStream outputStream = WriterHelper.writeRecordToStream(User.class, DataHelper.createUser());

        // DESERIALIZE USERS FROM binary
        assertEquals("3 Users have been created", 3, ReaderHelper.readFromStream(User.class, outputStream).size());
    }
}
