package io.debezium.connector.mysql.cube;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import org.arquillian.cube.docker.impl.docker.DockerClientExecutor;
import org.arquillian.cube.spi.CubeRegistry;
import org.jboss.arquillian.core.api.Instance;
import org.jboss.arquillian.core.api.annotation.Inject;
import org.jboss.arquillian.test.spi.TestEnricher;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.mysql.MySqlConnectorConfig;

public class DatabaseCubeTestEnricher implements TestEnricher {
    // private static final Logger logger =
    // Logger.getLogger(DatabaseCubeTestEnricher.class.getName());

    @Inject
    Instance<CubeRegistry> cubeRegistryInstance;

    @Inject
    private Instance<DockerClientExecutor> dockerClientExecutor;

    @Override
    public void enrich(final Object testCase) {
        try {
            List<Field> fields = ReflectionUtil.getFieldsOfType(testCase.getClass(), DatabaseCube.class);
            System.out.println(fields);
            for (final Field f : fields) {
                final Annotation[] annotations = f.getAnnotations();
                for (final Annotation a : annotations) {
                    if (ReflectionUtil.isAnnotationWithAnnotation(a.annotationType(), CubeReference.class)) {
                        final String cubeName = a.annotationType().getAnnotation(CubeReference.class).value();
                        System.out.println(cubeName);
                        f.set(testCase, new DatabaseCube() {
                            final DockerClientExecutor docker = dockerClientExecutor.get();

                            @Override
                            public String getCubeName() {
                                return cubeName;
                            }

                            @Override
                            public String getCubeIP() {
                                return docker.inspectContainer(getCubeName()).getNetworkSettings().getIpAddress();
                            }

                            @Override
                            public Builder configuration() {
                                return Configuration.create().with(MySqlConnectorConfig.HOSTNAME, getCubeIP())
                                        .with(MySqlConnectorConfig.PORT, 3306);
                            }
                        });
                    }
                }
            }
        } catch (final Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Object[] resolve(final Method method) {
        return new Object[method.getParameterTypes().length];
    }

}
