package fr.pomverte;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Test;

import fr.pomverte.helper.UserDataSetHelper;

/**
 * Avro Without Code Generation
 */
public class GenericRecordTest {

    private static final String USER_SCHEMA_PATH = "src/main/avro/user.avsc";
    
    @Test
    public void createSerializeDeserialize() throws IOException {
        // PARSE SCHEMA
        Schema schema = new Schema.Parser().parse(new File(USER_SCHEMA_PATH));

        List<GenericRecord> users = UserDataSetHelper.createRecord(schema);

        // Serialize user1 and user2 to disk
        File outputFile = new File("target/users.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, outputFile);
        dataFileWriter.append(users.get(0));
        dataFileWriter.append(users.get(1));
        dataFileWriter.close();

        // Deserialize users from disk
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        @SuppressWarnings("resource")
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(outputFile, datumReader);
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