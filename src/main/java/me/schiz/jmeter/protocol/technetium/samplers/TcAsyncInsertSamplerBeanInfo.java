package me.schiz.jmeter.protocol.technetium.samplers;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import java.beans.PropertyDescriptor;

public class TcAsyncInsertSamplerBeanInfo
        extends BeanInfoSupport  {
    public TcAsyncInsertSamplerBeanInfo() {
        super(TcAsyncInsertSampler.class);

        createPropertyGroup("options", new String[]{
                "source",
                "poolTimeout",
                "notifyOnlyArgentums"});
                //"includePoolTime"});

        createPropertyGroup("request", new String[]{
                "key",
                "keySerializerType",
                "columnParent",
                "column",
                "columnSerializerType",
                "timestamp",
                "value",
                "valueSerializerType",
                "consistencyLevel"});

        PropertyDescriptor p = property("source");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("notifyOnlyArgentums");
        p.setValue(NOT_UNDEFINED, Boolean.FALSE);
        p.setValue(DEFAULT, "");

//        p = property("includePoolTime");
//        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
//        p.setValue(DEFAULT, "TRUE");

        p = property("key");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("keySerializerType");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("columnParent");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("column");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("columnSerializerType");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("timestamp");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("value");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("valueSerializerType");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("consistencyLevel");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

//        p = property("poolTimeout");
//        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
//        p.setValue(DEFAULT, String.valueOf(TcCQL3StatementSampler.DEFAULT_POOL_TIMEOUT));
    }
}
