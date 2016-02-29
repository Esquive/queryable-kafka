package org.apache.kafka.common.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.ByteBufferOutputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created by ericfalk on 28/02/16.
 */
public class StringGzipDeserializer implements Deserializer<String> {
    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("deserializer.encoding");
        if (encodingValue != null && encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    @Override
    public String deserialize(String topic, byte[] data) {

        GZIPInputStream in = null;
        ByteBufferOutputStream out = null;

        try {
            if (data == null)
                return null;
            else {
                float rate = 0.5f * 1.05f;

                int b4 = data[data.length - 4];
                int b3 = data[data.length - 3];
                int b2 = data[data.length - 2];
                int b1 = data[data.length - 1];
                int val = (b1 << 24) | (b2 << 16) + (b3 << 8) + b4;

                out = new ByteBufferOutputStream(ByteBuffer.allocate(val + 30));
                in = new GZIPInputStream(new ByteArrayInputStream(data));

                int read = 0;
                byte[] buffer = new byte[1024];
                while ((read = in.read(buffer)) != -1) {
                    out.write(buffer, 0, read);
                }
                out.buffer().flip();
                return new String(out.buffer().array(), encoding);

            }
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + encoding);
        } catch (IOException e) {
            throw new SerializationException("Error when deserializing GZIP byte[] to string");
        }
    }

    @Override
    public void close() {
        //Nothing to do
    }
}
