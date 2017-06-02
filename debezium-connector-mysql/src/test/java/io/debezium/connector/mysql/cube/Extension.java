package io.debezium.connector.mysql.cube;

import org.jboss.arquillian.core.spi.LoadableExtension;
import org.jboss.arquillian.test.spi.TestEnricher;

public class Extension implements LoadableExtension {

    @Override
    public void register(final ExtensionBuilder builder) {
        builder.service(TestEnricher.class, DatabaseCubeTestEnricher.class);
    }

}
