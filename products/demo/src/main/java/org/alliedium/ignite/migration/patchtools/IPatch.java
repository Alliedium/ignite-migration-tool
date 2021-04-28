package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheMetaData;

public interface IPatch {

    ICacheMetaData transformMetaData(ICacheMetaData cacheMetaData);

    ICacheData transformData(ICacheData cacheData);
}
