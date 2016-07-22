package fr.pomverte;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
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
    public static <T extends SpecificRecordBase> List<T> readSpecificFromByte(Class<T> classz,
            byte[] avro) {
        DatumReader<T> datumReader = new SpecificDatumReader<>(classz);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avro, null);
        List<T> result = new ArrayList<>();
        try {
            while (true) {
                T deserialized = classz.getDeclaredConstructor().newInstance();
                datumReader.read(deserialized, decoder);
                result.add(deserialized);
            }
        } catch (EOFException e) {
            // the end of the decoder

        } catch (IOException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
        }
        return result;
    }

    public List<GenericRecord> readGenericFromFile(Schema schema, File file) {
        return ReaderHelper.readFromFile(GenericRecord.class, new GenericDatumReader<GenericRecord>(schema), file);
    }

    public static <T extends SpecificRecordBase> List<T> readSpecificFromFile(Class<T> classz, File file) {
        return ReaderHelper.readFromFile(classz, new SpecificDatumReader<>(classz), file);
    }

    private static <T> List<T> readFromFile(Class<T> classz, DatumReader<T> datumReader, File file) {
        List<T> result = new ArrayList<>();
        try (DataFileReader<T> dataFileReader = new DataFileReader<>(file, datumReader)) {
            T record = null;
            while (dataFileReader.hasNext()) {
                // Reuse user object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                result.add(dataFileReader.next(record));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

}
