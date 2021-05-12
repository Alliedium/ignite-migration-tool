package org.alliedium.ignite.migration.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class PathCombineTest {

    @Test
    public void testPathCombine() {
        Path path = Paths.get("src/");
        PathCombine pathCombine = new PathCombine(path);
        Assert.assertTrue(pathCombine.plus("test/resources").getPath().toString().contains("src/test/resources"));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testPathCombineDontAcceptsNull() {
        new PathCombine(null);
    }

    @Test
    public void testCombineMultipleDirectories() {
        Path path = Paths.get("src/");
        PathCombine pathCombine = new PathCombine(path);
        pathCombine = pathCombine.plus("test");
        Assert.assertTrue(pathCombine.getPath().toString().contains("src/test"));
        pathCombine = pathCombine.plus("resources");
        Assert.assertTrue(pathCombine.getPath().toString().contains("src/test/resources"));
    }
}