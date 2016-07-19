package fr.pomverte;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.junit.Test;

/**
 * Extract an Avro Schema using reflexion API
 */
public class ExtractSchemaTest {

    private static final String EXPECTED_SCHEMA = "{\"type\":\"record\",\"name\":\"ReversePojo\",\"namespace\":\"fr.pomverte\",\"fields\":[{\"name\":\"image\",\"type\":\"int\"},{\"name\":\"sound\",\"type\":\"string\"},{\"name\":\"experimentation\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Entry\",\"namespace\":\"java.util.Map$\",\"fields\":[]},\"java-class\":\"java.util.List\"}},{\"name\":\"atom\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"PairObjectObject\",\"namespace\":\"org.apache.avro.reflect\",\"fields\":[{\"name\":\"key\",\"type\":{\"type\":\"record\",\"name\":\"Object\",\"namespace\":\"java.lang\",\"fields\":[]}},{\"name\":\"value\",\"type\":\"java.lang.Object\"}]},\"java-class\":\"java.util.Map\"}}]}";

    @Test
    public void extractSchemaFromClassTest() {
        Schema schema = ReflectData.get().getSchema(ReversePojo.class);
        assertNotNull(schema);
        assertEquals(EXPECTED_SCHEMA, schema.toString());
    }

}
