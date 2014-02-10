package com.github.eslab;


import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

import static org.fest.assertions.Assertions.assertThat;

public class BdbStoreTest {

    private BdbStore bdbStore;

    @Before
    public void setUp() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        Collection<Serializer> serializers = Lists.newArrayList();
        serializers.add(new FatSimpleSerializer());
        Stopwatch stopwatch = Stopwatch.createStarted();
        bdbStore = new BdbStore(tmpDir, serializers);
        System.out.println("db startup in " + stopwatch);
    }


    @Test
    public void should_store_an_event_and_be_able_to_load_back() {
        // given
        UUID id = UUID.randomUUID();
        SimpleEvent event = new SimpleEvent(id, 0L, 0L);

        // when
        bdbStore.store(event);
        List<Event> events = bdbStore.loadEventStream(id);

        // then
        assertThat(events).hasSize(1);

    }

    @Test
    @Ignore
    public void bench() {
        for (int k=0; k<5; k++ ) {
            List<UUID> ids = new ArrayList<>();
        Stopwatch stopwatch = Stopwatch.createStarted();
        long maxTime = 0;
        for (int i =0; i<1000; i++) {
            UUID id = UUID.randomUUID();
            ids.add(id);
            for (int j =0; j<100; j++) {
                long start = System.nanoTime();
                bdbStore.store(new SimpleEvent(id, System.currentTimeMillis(), j));
                long delta = System.nanoTime() - start;
                if (delta > maxTime) {
                    maxTime = delta;
                }
            }
        }
        System.out.println("100000 insertions in " + stopwatch);
        System.out.println("max time " + maxTime + " ns");
        maxTime = 0;
        stopwatch = Stopwatch.createStarted();
        for (int i =0; i<1000; i++) {
            long start = System.nanoTime();
            bdbStore.loadEventStream(ids.get(i));
            long delta = System.nanoTime() - start;
            if (delta > maxTime) {
                maxTime = delta;
            }
        }
        System.out.println("1000 reads in " + stopwatch);
        System.out.println("max time " + maxTime + " ns");
        }

    }
}
