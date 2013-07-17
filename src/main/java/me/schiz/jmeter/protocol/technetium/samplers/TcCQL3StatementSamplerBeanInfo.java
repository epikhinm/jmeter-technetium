package me.schiz.jmeter.protocol.technetium.samplers;

import org.apache.jmeter.testbeans.BeanInfoSupport;

import java.beans.PropertyDescriptor;

public class TcCQL3StatementSamplerBeanInfo
    extends BeanInfoSupport  {
    protected TcCQL3StatementSamplerBeanInfo() {
        super(TcCQL3StatementSampler.class);

        createPropertyGroup("options", new String[]{
                "source",
                "query",
                "poolTimeout"});

        PropertyDescriptor p = property("source");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("query");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("poolTimeout");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, String.valueOf(TcCQL3StatementSampler.DEFAULT_POOL_TIMEOUT));
    }
}
