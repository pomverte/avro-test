package fr.pomverte;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import com.cloudera.blog.Employee1;
import com.cloudera.blog.Employee3;

public class SchemaEvolutionTest {

    @Test
    public void fileSerializationTest() throws IOException {

        Employee1 employee = new Employee1();
        employee.setName("JSAI");
        employee.setAge(27);
        List<String> emails = new ArrayList<String>();
        emails.add("jsai@wonderland.com");
        employee.setEmails(emails);

        emails.clear();
        emails.add("boubou@tomorrowland.com");
        employee.setBoss(Employee1.newBuilder().setAge(31).setName("BOUBOU").setEmails(emails).build());

        // SERIALIZING with schema version 1
        DatumWriter<Employee1> writer = new SpecificDatumWriter<Employee1>(Employee1.class);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        writer.write(employee, encoder);
        // flush the binary code into the outputstream
        encoder.flush();

        // DESERIALIZE with schema version 2
        Schema writerSchema = Employee1.getClassSchema();
        Schema readerSchema = Employee3.getClassSchema();

        // assertEquals(SchemaCompatibilityType.COMPATIBLE,
        // SchemaCompatibility.checkReaderWriterCompatibility(readerSchema, writerSchema));

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(writerSchema, readerSchema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
        System.out.println(reader.read(new Employee3(), decoder));
    }

}
