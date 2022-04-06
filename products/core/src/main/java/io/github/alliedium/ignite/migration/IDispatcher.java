package io.github.alliedium.ignite.migration;

/**
 * IDispatcher is the main dto distributor, provides message channel between publishers and subscribers.
 */
public interface IDispatcher<DTO> {

    void publish(DTO dto);

    void subscribe(IDataWriter<DTO> consumer);

    void finish();
}
