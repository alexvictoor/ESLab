package com.github.eslab;


import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.fest.assertions.Assertions.assertThat;

public class LdbStoreTest {

    private LdbStore ldbStore;

    @Before
    public void setUp() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        Collection<Serializer> serializers = Lists.newArrayList();
        serializers.add(new SimpleSerializer());
        Stopwatch stopwatch = Stopwatch.createStarted();
        ldbStore = new LdbStore(tmpDir, serializers);
        System.out.println("db startup in " + stopwatch);
    }

    @After
    public void tearDown() {

    }


    @Test
    public void should_store_an_event_and_be_able_to_load_back() {
        // given
        UUID id = UUID.randomUUID();
        SimpleEvent event = new SimpleEvent(id, 0L, 0L);

        // when
        ldbStore.store(event);
        List<Event> events = ldbStore.loadEventStream(id);

        // then
        assertThat(events).hasSize(1);

    }

    @Test
    @Ignore
    public void bench() {
        for (int k=0; k<5; k++ ) {
            List<UUID> ids = new ArrayList<>();
            Stopwatch stopwatch = Stopwatch.createStarted();
            for (int i =0; i<1000; i++) {
                UUID id = UUID.randomUUID();
                ids.add(id);
                for (int j =0; j<100; j++) {
                    ldbStore.store(new SimpleEvent(id, System.currentTimeMillis(), j));
                }
            }
            System.out.println("100000 insertions in " + stopwatch);
            stopwatch = Stopwatch.createStarted();
            for (int i =0; i<1000; i++) {
                ldbStore.loadEventStream(ids.get(i));
            }
            System.out.println("1000 reads in " + stopwatch);
        }

    }
}
