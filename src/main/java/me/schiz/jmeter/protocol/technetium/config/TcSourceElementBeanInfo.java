package me.schiz.jmeter.protocol.technetium.config;

import org.apache.jmeter.testbeans.BeanInfoSupport;

import java.beans.PropertyDescriptor;

/**
 * Created with IntelliJ IDEA.
 * User: schizophrenia
 * Date: 7/16/13
 * Time: 6:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class TcSourceElementBeanInfo
    extends BeanInfoSupport {
    protected TcSourceElementBeanInfo() {
        super(TcSourceElement.class);

        createPropertyGroup("options", new String[]{
                "source",
                "keyspace",
                "hosts",
                "maxSelectors",
                "maxInstances",
                "timeout"});

        PropertyDescriptor p = property("source");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("keyspace");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("hosts");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("maxSelectors");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("maxInstances");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");

        p = property("timeout");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
    }
}
