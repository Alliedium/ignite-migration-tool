package org.alliedium.ignite.migration.util;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public final class PathCombine {

    private final Path path;

    public PathCombine(Path path) {
        this(path, true);
    }

    private PathCombine(Path path, boolean createNew) {
        Objects.requireNonNull(path);
        if (createNew) {
            this.path = Paths.get(path.toUri());
        } else {
            this.path = path;
        }
    }

    public PathCombine plus(String path) {
        return new PathCombine(Paths.get(this.path.toString(), path), false);
    }

    public Path getPath() {
        return Paths.get(path.toUri());
    }
}
