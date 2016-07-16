package fr.pomverte;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import example.avro.User;
import example.avro.User2;

/**
 * {@link https://docs.oracle.com/cd/NOSQL/html/GettingStartedGuide/schemaevolution.html}
 */
public class SchemaEvolutionTest {

    @Test
    public void serialiazeV1derializeV2() throws IOException {
        // Serialize data with shema version 1
        File outputFile = WriterHelper.writeRecordToFile(User.class, DataHelper.createUser(),
                new File("target/users.avro"));

        // Deserialize with schema version 2
        List<User2> readFromFile = ReaderHelper.readSpecificFromFile(User2.class, outputFile);

        // check number of records
        assertEquals("3 Users have been recover from file", 3, readFromFile.size());
        for (User2 user2 : readFromFile) {
            // check default value
            assertEquals("The surname should be set with a default value", "johndoe", user2.getSurname());
        }
    }
}
