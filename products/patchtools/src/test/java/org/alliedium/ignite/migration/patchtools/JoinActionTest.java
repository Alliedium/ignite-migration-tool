package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.test.model.Fabricator;
import org.alliedium.ignite.migration.test.model.Product;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JoinActionTest extends BaseTest {

    @Test
    public void testJoinAction(Method method) {
        String productCacheName = method.getName() + "Product";
        String fabricatorCacheName = method.getName() + "Fabricator";

        CacheConfiguration<Long, Product> productConfig = createProductCacheConfiguration(productCacheName);
        CacheConfiguration<Long, Fabricator> fabricatorConfig = createFabricatorCacheConfiguration(fabricatorCacheName);

        clientAPI.getIgnite().createCache(productConfig);
        clientAPI.getIgnite().createCache(fabricatorConfig);

        IgniteCache<Long, Product> productCache = clientAPI.getIgnite().cache(productCacheName);
        IgniteCache<Long, Fabricator> fabricatorCache = clientAPI.getIgnite().cache(fabricatorCacheName);

        Fabricator nvidia = new Fabricator(1, "NVIDIA", "KR");
        Fabricator intel = new Fabricator(2, "INTEL", "US");
        Fabricator amd = new Fabricator(3, "AMD", "UK");

        fabricatorCache.put(nvidia.getId(), nvidia);
        fabricatorCache.put(intel.getId(), intel);
        fabricatorCache.put(amd.getId(), amd);

        List<Fabricator> fabricators = Stream.of(nvidia, intel, amd).collect(Collectors.toList());
        List<Product> products = new ArrayList<>();

        for(long i = 1; i < 51; i++) {
            Collections.shuffle(fabricators);
            Product product = Product.builder()
                    .id(i)
                    .fabricatorId(fabricators.get(0).getId())
                    .name("testProduct" + i)
                    .price(100 + i)
                    .build();
            products.add(product);
            productCache.put(i, product);
        }

        controller.serializeDataToAvro(source.getPath());

        PatchContext context = new PatchContext(source, destination);
        context.prepare();

        context.patchCachesWhichEndWith(productCacheName, cachePath -> {
            String fabricatorCachePath = cachePath.substring(0, cachePath.lastIndexOf("/") + 1)
                    + fabricatorCacheName;

            TransformAction<TransformOutput> productAction = new SelectAction.Builder()
                    .context(context)
                    .fields("key", "id", "fabricatorId", "name", "price")
                    .from(cachePath)
                    .build();

            TransformAction<TransformOutput> fabricatorAction = new SelectAction.Builder()
                    .context(context)
                    .fields("id", "name", "country")
                    .from(fabricatorCachePath)
                    .build();

            fabricatorAction = new RenameFieldAction.Builder()
                    .action(fabricatorAction)
                    .renameField("id", "fabricatorId")
                    .build();
            fabricatorAction = new RenameFieldAction.Builder()
                    .action(fabricatorAction)
                    .renameField("name", "fabricatorName")
                    .build();

            TransformAction<TransformOutput> action = new JoinAction.Builder()
                    .join(productAction, fabricatorAction)
                    .on("fabricatorId")
                    .build();

            action = new RenameCacheAction.Builder()
                    .action(action)
                    .newTableName("PRODUCTS_FABRICATORS")
                    .newCacheName(productCacheName + "Fabricator")
                    .build();

            new CacheWriter(action).writeTo(destination.plus(productCacheName + "Fabricator").getPath().toString());
        });

        context.getPipeline().run().waitUntilFinish();

        controller.deserializeDataFromAvro(destination.getPath());

        Map<Long, Product> productMap = products.stream().collect(
                Collectors.toMap(Product::getId, Function.identity()));
        Map<Long, Fabricator> fabricatorMap = fabricators.stream().collect(
                Collectors.toMap(Fabricator::getId, Function.identity()));

        clientAPI.getIgnite().cache(productCacheName + "Fabricator").withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    long fabricatorId = entry.getValue().field("fabricatorId");
                    long productId = entry.getValue().field("id");
                    Product product = productMap.get(productId);
                    Assert.assertEquals(product.getId(), (long)entry.getValue().field("id"));
                    Assert.assertEquals(product.getPrice(), (long)entry.getValue().field("price"));
                    Assert.assertEquals(product.getFabricatorId(), (long)entry.getValue().field("fabricatorId"));
                    Assert.assertEquals(product.getName(), entry.getValue().field("name"));

                    Fabricator fabricator = fabricatorMap.get(fabricatorId);
                    Assert.assertEquals(fabricator.getId(), (long)entry.getValue().field("fabricatorId"));
                    Assert.assertEquals(fabricator.getName(), entry.getValue().field("fabricatorName"));
                    Assert.assertEquals(fabricator.getCountry(), entry.getValue().field("country"));
                });
    }

    private CacheConfiguration<Long, Product> createProductCacheConfiguration(String productCacheName) {
        QueryEntity queryEntity = new QueryEntity()
                .setValueType(Product.class.getName())
                .setKeyType(Long.class.getName())
                .addQueryField("name", String.class.getName(), null)
                .addQueryField("id", long.class.getName(), null)
                .addQueryField("price", long.class.getName(), null)
                .addQueryField("fabricatorId", long.class.getName(), null);

        CacheConfiguration<Long, Product> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setQueryEntities(Collections.singleton(queryEntity));
        cacheConfiguration.setName(productCacheName);

        return cacheConfiguration;
    }

    private CacheConfiguration<Long, Fabricator> createFabricatorCacheConfiguration(String fabricatorCacheName) {
        QueryEntity queryEntity = new QueryEntity()
                .setValueType(Fabricator.class.getName())
                .setKeyType(Long.class.getName())
                .addQueryField("name", String.class.getName(), null)
                .addQueryField("id", long.class.getName(), null)
                .addQueryField("country", String.class.getName(), null);

        CacheConfiguration<Long, Fabricator> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setQueryEntities(Collections.singleton(queryEntity));
        cacheConfiguration.setName(fabricatorCacheName);

        return cacheConfiguration;
    }
}
