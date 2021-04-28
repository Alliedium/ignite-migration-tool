package org.alliedium.ignite.migration.dao.dataaccessor;

import java.util.ArrayList;
import java.util.List;

public interface IgniteAtomicLongNamesProvider {
    IgniteAtomicLongNamesProvider EMPTY = ArrayList::new;

    List<String> getAtomicNames();
}
