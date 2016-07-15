package fr.pomverte;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.junit.Test;

public class SchemaCompatibilityTest {

    @Test
    public void incompatible() throws IOException {
        // FIXME I don't known why aliases seems to fuck up schema backward compatibility
        Schema readerSchema = new Schema.Parser().parse(new File("src/main/avro/employee_1.avsc"));
        Schema writerSchema = new Schema.Parser().parse(new File("src/main/avro/employee_2.avsc"));
        assertEquals(SchemaCompatibilityType.INCOMPATIBLE,
                SchemaCompatibility.checkReaderWriterCompatibility(readerSchema, writerSchema).getType());
    }

}
