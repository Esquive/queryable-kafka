package org.apache.kafka.common.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.ByteBufferOutputStream;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * Created by ericfalk on 28/02/16.
 */
public class StringGzipSerializer implements Serializer<String> {
    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("serializer.encoding");
        if (encodingValue != null && encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    @Override
    public byte[] serialize(String topic, String data) {

        float rate = 0.5f * 1.05f;

        if (data == null)
            return null;
        else {
            ByteBufferOutputStream value = null;
            GZIPOutputStream out = null;
            try {

                int comLength = (int)(data.length() * rate);
                value = new ByteBufferOutputStream(ByteBuffer.allocate(comLength));

                out = new GZIPOutputStream(value);
                out.write(data.getBytes(encoding));
                out.flush();
                out.close();
                value.close();
                value.buffer().flip();

                byte[] result = new byte[value.buffer().remaining()];
                value.buffer().get(result, 0, result.length);
                return result;
            } catch (UnsupportedEncodingException e) {
                throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " + encoding);
            } catch (IOException e) {
                throw new SerializationException("Error when serializing string to gzip byte[].");
            } finally {
                if (out != null) {
                    try {
                        out.flush();
                        out.close();
                    }  catch (IOException e) {
                        //Nothing to do here
                    }
                }

            }
        }
    }

    @Override
    public void close() {
        //Nothing to do here
    }
}
