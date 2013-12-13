package me.schiz.jmeter.protocol.technetium.pool;

import com.netflix.astyanax.serializers.*;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class NetflixUtils {
    public static final String NEW_LINE = System.getProperty("line.separator");

    public static String getStackTrace(Throwable aThrowable)
    {
        final StringBuilder result = new StringBuilder("ERROR: ");
        result.append(aThrowable.toString());
        result.append(NEW_LINE);
        // add each element of the stack trace
        for (StackTraceElement element : aThrowable.getStackTrace())
        {
            result.append(element.toString());
            result.append(NEW_LINE);
        }
        return result.toString();
    }

    @SuppressWarnings("rawtypes")
    public static Map<String, AbstractSerializer> serializers = new HashMap<String, AbstractSerializer>();
    static
    {
        serializers.put("StringSerializer", StringSerializer.get());
        serializers.put("IntegerSerializer", IntegerSerializer.get());
        serializers.put("LongSerializer", LongSerializer.get());
        serializers.put("BooleanSerializer", BooleanSerializer.get());
        serializers.put("DoubleSerializer", DoubleSerializer.get());
        serializers.put("DateSerializer", DateSerializer.get());
        serializers.put("FloatSerializer", FloatSerializer.get());
        serializers.put("ShortSerializer", ShortSerializer.get());
        serializers.put("UUIDSerializer", UUIDSerializer.get());
        serializers.put("BigIntegerSerializer", BigIntegerSerializer.get());
        serializers.put("CharSerializer", CharSerializer.get());
        serializers.put("AsciiSerializer", AsciiSerializer.get());
        serializers.put("BytesArraySerializer", BytesArraySerializer.get());
    }

    public static Object convert(String text, String kSerializerType)
    {
        if (kSerializerType.equals("StringSerializer"))
        {
            return text;
        }
        else if (kSerializerType.equals("IntegerSerializer"))
        {
            return Integer.parseInt(text);
        }
        else if (kSerializerType.equals("LongSerializer"))
        {
            return Long.parseLong(text);
        }
        else if (kSerializerType.equals("BooleanSerializer"))
        {
            return Boolean.parseBoolean(text);
        }
        else if (kSerializerType.equals("DoubleSerializer"))
        {
            return Double.parseDouble(text);
        }
        else if (kSerializerType.equals("BooleanSerializer"))
        {
            return Boolean.parseBoolean(text);
        }
        else if (kSerializerType.equals("DateSerializer"))
        {
            return Date.parse(text);
        }
        else if (kSerializerType.equals("FloatSerializer"))
        {
            return Float.parseFloat(text);
        }
        else if (kSerializerType.equals("ShortSerializer"))
        {
            return Short.parseShort(text);
        }
        else if (kSerializerType.equals("UUIDSerializer"))
        {
            return UUID.fromString(text);
        }
        else if (kSerializerType.equals("BigIntegerSerializer"))
        {
            return new BigInteger(text);
        }
        else if (kSerializerType.equals("CharSerializer"))
        {
            // TODO fix it.
            return text;
        }
        else if (kSerializerType.equals("AsciiSerializer"))
        {
            return text;
        }
        else if (kSerializerType.equals("BytesArraySerializer"))
        {
            try {
                return Hex.decodeHex(text.toCharArray());
            } catch (DecoderException e) {
                return null;
            }
        }
        return serializers.get(kSerializerType).fromString(text);
    }

}