package org.alliedium.ignite.migration;

/**
 * IDispatcher is the main dto distributor, provides message channel between publishers and subscribers.
 * @param <DTO>
 */
public interface IDispatcher<DTO> {

    void publish(DTO dto);

    void subscribe(IDataWriter<DTO> consumer);

    void finish();
}
