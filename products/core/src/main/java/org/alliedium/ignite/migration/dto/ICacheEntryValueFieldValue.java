package org.alliedium.ignite.migration.dto;

import java.util.Optional;

/**
 * Provides an access to cache entry field value.
 * Can be accessed from parent entity {@link ICacheEntryValueField}, which includes cache entry field meta-data and
 * current ICacheEntryValueFieldValue (representing cache entry field value).
 */
public interface ICacheEntryValueFieldValue {

    Optional<Object> getValue();

}
