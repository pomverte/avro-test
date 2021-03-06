package fr.pomverte;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.Test;

/**
 * Avro Without Code Generation
 */
public class GenericRecordTest {

    private static final String USER_SCHEMA_PATH = "src/main/avro/user.avsc";
    
    @Test
    public void createSerializeDeserialize() throws IOException {
        // PARSE SCHEMA
        Schema schema = new Schema.Parser().parse(new File(USER_SCHEMA_PATH));

        List<GenericRecord> records = DataHelper.createRecord(schema);

        // Serialize user1 and user2 to disk
        File outputFile = WriterHelper.writeGenericToFile(schema, records, new File("target/users.avro"));

        // Deserialize users from disk
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        @SuppressWarnings("resource")
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(outputFile, datumReader);
        GenericRecord user = null;
        int counter = 0;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            user = dataFileReader.next(user);
            System.out.println(user);
            counter++;
        }
        assertEquals("2 Users have been created", 2, counter);
    }

}
