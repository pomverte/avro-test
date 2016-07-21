package fr.pomverte;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

public class WriterHelper {

    public static <T extends SpecificRecordBase> File writeRecordToFile(Class<T> classz, List<T> records, File file) {
        DatumWriter<T> userDatumWriter = new SpecificDatumWriter<>(classz);
        try {
            DataFileWriter<T> dataFileWriter = new DataFileWriter<>(userDatumWriter);
            File outputFile = new File("target/users.avro");
            T record1 = records.get(0);
            dataFileWriter.create(record1.getSchema(), outputFile);
            dataFileWriter.append(record1);
            dataFileWriter.append(records.get(1));
            dataFileWriter.append(records.get(2));
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    public static File writeGenericToFile(Schema schema, List<GenericRecord> records, File outputFile) {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        try {
            dataFileWriter.create(schema, outputFile);
            for (GenericRecord record : records) {
                dataFileWriter.append(record);
            }
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // don't need finally outputStream.close() => Java7 try-with-resources Statement
        return outputFile;
    }

    public static <T extends SpecificRecordBase> ByteArrayOutputStream writeRecordToStream(Class<T> classz,
            List<T> records) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            DatumWriter<T> userDatumWriter = new SpecificDatumWriter<>(classz);
            for (T record : records) {
                userDatumWriter.write(record, encoder);
            }
            // flush the binary code into the outputstream
            encoder.flush();
            return outputStream;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param schema
     * @param classz
     * @param pretty
     * @param record
     * @return
     */
    public static <T extends SpecificRecordBase> String writeRecordToJson(Schema schema, Class<T> classz,
            boolean pretty, T record) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DatumWriter<T> datumWriter = new SpecificDatumWriter<>(classz);
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, outputStream, pretty);
            datumWriter.write(record, encoder);
            // flush the binary code into the outputstream
            encoder.flush();
            outputStream.flush();
            return new String(outputStream.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param schema
     * @param classz
     * @param pretty
     * @param records
     * @return
     */
    public static <T extends SpecificRecordBase> String writeRecordToJson(Schema schema, Class<T> classz,
            boolean pretty, List<T> records) {
        List<String> result = new ArrayList<>();
        for (T record : records) {
            result.add(writeRecordToJson(schema, classz, pretty, record));
        }
        return result.toString();
    }
}
