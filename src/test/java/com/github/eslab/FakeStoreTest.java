package com.github.eslab;


import com.google.common.base.Stopwatch;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.fest.assertions.Assertions.assertThat;

public class FakeStoreTest {

    private FakeStore fakeStore = new FakeStore(new SimpleSerializer());


    @Test
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
                fakeStore.store(new SimpleEvent(id, System.currentTimeMillis(), j));
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
            fakeStore.loadEventStream(ids.get(i));
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
