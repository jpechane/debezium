package io.debezium.connector.mysql.cube;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

public class ReflectionUtil {

    public static List<Field> getFieldsOfType(final Class<?> source, final Class<?> requestedClass) {
        List<Field> declaredAccessibleFields = AccessController.doPrivileged(new PrivilegedAction<List<Field>>() {
            public List<Field> run() {
                List<Field> foundFields = new ArrayList<Field>();
                Class<?> nextSource = source;
                while (nextSource != Object.class) {
                    for (Field field : nextSource.getDeclaredFields()) {
                        if (field.getType().isAssignableFrom(requestedClass)) {
                            if (!field.isAccessible()) {
                                field.setAccessible(true);
                            }
                            foundFields.add(field);
                        }
                    }
                    nextSource = nextSource.getSuperclass();
                }
                return foundFields;
            }
        });
        return declaredAccessibleFields;
    }

    public static boolean isAnnotationWithAnnotation(final Class<? extends Annotation> source,
            final Class<? extends Annotation> annotationClass) {
        return org.arquillian.cube.impl.util.ReflectionUtil.isClassWithAnnotation(source, annotationClass);
    }
}
