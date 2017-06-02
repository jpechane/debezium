package io.debezium.connector.mysql.cube;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(FIELD)
@CubeReference("mysql-server")
public @interface DefaultDatabase {
}
