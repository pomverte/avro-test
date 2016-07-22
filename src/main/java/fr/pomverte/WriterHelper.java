package fr.pomverte;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

public class WriterHelper {

    private static byte[] NO_BYTE_ARRAY = {};
    
    public static <T extends SpecificRecordBase> File writeRecordToFile(Class<T> classz, List<T> records, File file) {
        DatumWriter<T> userDatumWriter = new SpecificDatumWriter<>(classz);
        try (DataFileWriter<T> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
            File outputFile = new File("target/users.avro");
            T record1 = records.get(0);
            dataFileWriter.create(record1.getSchema(), outputFile);
            dataFileWriter.append(record1);
            dataFileWriter.append(records.get(1));
            dataFileWriter.append(records.get(2));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    public static File writeGenericToFile(Schema schema, List<GenericRecord> records, File outputFile) {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outputFile);
            for (GenericRecord record : records) {
                dataFileWriter.append(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return outputFile;
    }

    public static <T extends SpecificRecordBase> byte[] writeRecordToStream(Class<T> classz,
            List<T> records) {
        byte[] result;
        DatumWriter<T> userDatumWriter = new SpecificDatumWriter<>(classz);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            for (T record : records) {
                userDatumWriter.write(record, encoder);
            }
            // flush the binary code into the outputstream
            encoder.flush();
            result = outputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return NO_BYTE_ARRAY;
        }
        return result;
    }
}
