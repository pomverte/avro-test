package fr.pomverte;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;

public class ReaderHelper {

    /**
     * DESERIALIZE SpecificRecord FROM binary stream
     * 
     * @param outputStream
     * @param classz
     */
    public static <T extends SpecificRecordBase> List<T> readFromStream(Class<T> classz,
            ByteArrayOutputStream outputStream) {
        DatumReader<T> datumReader = new SpecificDatumReader<>(classz);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
        List<T> result = new ArrayList<>();
        try {
            while (true) {
                T deserialized = classz.getDeclaredConstructor().newInstance();
                datumReader.read(deserialized, decoder);
                result.add(deserialized);
            }
        } catch (IOException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static <T extends SpecificRecordBase> List<T> readFromFile(Class<T> classz, File outputFile) {
        DatumReader<T> userDatumReader = new SpecificDatumReader<>(classz);
        List<T> result = new ArrayList<>();
        try {
            @SuppressWarnings("resource")
            DataFileReader<T> dataFileReader = new DataFileReader<>(outputFile, userDatumReader);
            T record = null;
            while (dataFileReader.hasNext()) {
                // Reuse user object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                record = dataFileReader.next(record);
                result.add(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
