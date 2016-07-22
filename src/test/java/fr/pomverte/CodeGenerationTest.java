package fr.pomverte;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;

import org.junit.Test;

import example.avro.User;

/**
 * {@link https://avro.apache.org/docs/current/gettingstartedjava.html}
 *
 */
public class CodeGenerationTest {

    @Test
    public void fileSerializationTest() {
        // SERIALIZING
        // Serialize user1, user2 and user3 to disk
        File outputFile = WriterHelper.writeRecordToFile(User.class, DataHelper.createUser(),
                new File("target/users.avro"));

        // DESERIALIZE USERS FROM DISK
        List<User> readFromFile = ReaderHelper.readSpecificFromFile(User.class, outputFile);
        assertEquals("3 Users have been created", 3, readFromFile.size());
    }

    @Test
    public void streamSerializationTest() {
        // SERIALIZING
        // Serialize user1, user2 and user3 into a stream
        byte[] avro = WriterHelper.writeRecordToStream(User.class, DataHelper.createUser());

        // DESERIALIZE USERS FROM binary
        List<User> users = ReaderHelper.readSpecificFromByte(User.class, avro);
        assertEquals("3 Users have been created", 3, users.size());
    }
}
