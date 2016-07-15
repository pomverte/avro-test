package fr.pomverte;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

public class WriterHelper {

    public static <T extends SpecificRecordBase> File writeRecordToFile(Class<T> classz,
            List<T> records, File file) {
        DatumWriter<T> userDatumWriter = new SpecificDatumWriter<>(classz);
        try {
            DataFileWriter<T> dataFileWriter = new DataFileWriter<>(userDatumWriter);
            File outputFile = new File("target/users.avro");
            T user1 = records.get(0);
            dataFileWriter.create(user1.getSchema(), outputFile);
            dataFileWriter.append(user1);
            dataFileWriter.append(records.get(1));
            dataFileWriter.append(records.get(2));
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    public static <T extends SpecificRecordBase> ByteArrayOutputStream writeRecordToStream(Class<T> classz,
            List<T> records) {
        DatumWriter<T> userDatumWriter = new SpecificDatumWriter<>(classz);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            for (T record : records) {
                userDatumWriter.write(record, encoder);
            }
            // flush the binary code into the outputstream
            encoder.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return outputStream;
    }
}
