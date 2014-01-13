package com.github.eslab;


import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class Bench {

    private final Store store;
    private final int nbLoop;
    private final int aggregateNumber;
    private final int readWriteRatio;
    private List<UUID> ids;
    private List<Integer> nbRunPerLoop;


    public Bench(Store store, int nbLoop, int aggregateNumber, int nbWritePerRead) {

        this.store = store;
        this.nbLoop = nbLoop;
        this.aggregateNumber = aggregateNumber;
        this.readWriteRatio = nbWritePerRead;
    }

    public void run() {
        initDataset();
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int nbRun : nbRunPerLoop) {
            for (int i = 0; i < nbRun; i++) {
                UUID uuid = ids.get(i);
                store.loadEventStream(uuid);
                for (int j=0; j < readWriteRatio; j++) {
                    store.store(new SimpleEvent(uuid, System.currentTimeMillis(), (i* readWriteRatio)+j));
                }
            }
        }
        try {
            store.close();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        System.out.println("Bench done in " + stopwatch);
    }

    private void initDataset() {
        ids = new ArrayList<>();
        for (int i=0; i<aggregateNumber; i++) {
            ids.add(UUID.randomUUID());
        }
        nbRunPerLoop = new ArrayList<>();
        int nbWrite = 0;
        for (int i=0; i<nbLoop; i++) {
            int nbRun = aggregateNumber - (i % (aggregateNumber - 1));
            nbRunPerLoop.add(nbRun);
            nbWrite += nbRun;
        }

        System.out.println("NB write: " + nbWrite);
        System.out.println("NB read: " + (nbWrite * readWriteRatio));

    }

    public static void main(String[] args) {
        String tmpDir = System.getProperty("java.io.tmpdir") + File.separator + "bench";
        Collection<Serializer> serializers = Lists.newArrayList();
        serializers.add(new SimpleSerializer());
        BdbStore bdbStore = new BdbStore(tmpDir, serializers);
        LdbJniStore ldbStore = new LdbJniStore(tmpDir, serializers);
        MapDbStore mapDbStore = new MapDbStore(tmpDir, serializers);
        System.out.println("LevelDB");
        new Bench(ldbStore, 1000, 100, 10).run();
        System.out.println("BerkeleyDB");
        new Bench(bdbStore, 1000, 100, 10).run();
        System.out.println("MapDB");
        new Bench(mapDbStore, 1000, 100, 10).run();
        bdbStore = new BdbStore(tmpDir, serializers);
        ldbStore = new LdbJniStore(tmpDir, serializers);
        mapDbStore = new MapDbStore(tmpDir, serializers);
        System.out.println("LevelDB");
        new Bench(ldbStore, 1000, 100, 10).run();
        System.out.println("BerkeleyDB");
        new Bench(bdbStore, 1000, 100, 10).run();
        System.out.println("MapDB");
        new Bench(mapDbStore, 1000, 100, 10).run();
    }
}
