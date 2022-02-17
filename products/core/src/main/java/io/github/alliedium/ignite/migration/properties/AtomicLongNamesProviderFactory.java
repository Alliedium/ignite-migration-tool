package io.github.alliedium.ignite.migration.properties;

import io.github.alliedium.ignite.migration.IgniteAtomicLongNamesProvider;
import org.apache.ignite.Ignite;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class AtomicLongNamesProviderFactory {

    private final Ignite ignite;

    public AtomicLongNamesProviderFactory(Ignite ignite) {
        this.ignite = ignite;
    }

    public IgniteAtomicLongNamesProvider create(PropertiesResolver propertiesResolver) {
        Optional<String> classOptional = propertiesResolver.getIgniteAtomicLongNamesProviderClass();
        if (classOptional.isPresent()) {
            Class<?> clazz = loadClass(classOptional.get());
            return create(clazz);
        }

        List<String> atomicLongNames = propertiesResolver.getAtomicLongNames();

        return () -> atomicLongNames;
    }

    private IgniteAtomicLongNamesProvider create(Class<?> clazz) {
        if (!IgniteAtomicLongNamesProvider.class.isAssignableFrom(clazz)) {
            throw new IllegalStateException(String.format("Provided %s does not implement %s",
                    clazz, IgniteAtomicLongNamesProvider.class));
        }

        return (IgniteAtomicLongNamesProvider) createInstance(clazz, ignite);
    }

    private static <T> T createInstance(Class<T> clazz, Object... args) {
        try {
            if (args.length != 0) {
                Class<?>[] argsClasses = Arrays.stream(args).map(Object::getClass).toArray(Class<?>[]::new);
                for (Constructor<?> ctor : clazz.getDeclaredConstructors()) {
                    if (constructorMatches(ctor, argsClasses)) {
                        return (T) ctor.newInstance(args);
                    }
                }
            }

            Constructor<T> constructor = clazz.getDeclaredConstructor();
            return constructor.newInstance();
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                    String.format("Cannot find constructor for provided %s", clazz), e);
        } catch (InvocationTargetException | InstantiationException e) {
            throw new IllegalStateException(e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                    String.format("Cannot instantiate %s cause it has no public constructor", clazz.getName()), e);
        }
    }

    private static boolean constructorMatches(Constructor<?> constructor, Class<?>[] argsClasses) {
        Class<?>[] parameterTypes = constructor.getParameterTypes();
        if (parameterTypes.length != argsClasses.length) {
            return false;
        }
        for (int i = 0; i < argsClasses.length; i++) {
            if (!parameterTypes[i].isAssignableFrom(argsClasses[i])) {
                return false;
            }
        }

        return true;
    }

    private static Class<?> loadClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }
}
