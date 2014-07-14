package org.hazelcast.mapdb;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.MultiMap;

public class Benchmark {
    
    public static void main(String[] args) {
        
        int threads = 5;
        
        for ( int thread=1; thread<=threads; thread++ ) {
            startNodeAndThread(thread);
        }
    }

    private static void startNodeAndThread(int threadId) {
        Thread thread = new Thread(makeRunnable(threadId));
        thread.start();
    }

    private static Runnable makeRunnable(final int threadId) {
        return new Runnable() {
            
            
            @Override
            public void run() {
                Config cfg = new Config();
                
                HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
        
                IQueue<Object> queue = instance.getQueue("queue");
                IMap<Object, Object> map = instance.getMap("map");
                MultiMap<Object, Object> mmap = instance.getMultiMap("mmap");
        
                final long timeLen = 60*60*1000;
                final long startTime =System.currentTimeMillis();
        
                String str1 = getLongString(0,10000);
                String str2 = getLongString(1,10001);
                
                long i=0;
                int gap = 10000;
                for(;startTime+timeLen>System.currentTimeMillis();i++){
                    if(i > 0 && i % 1000==0) {
                        long soFar = System.currentTimeMillis()-startTime;
                        try {
                            System.out.printf("THREAD %d, ITEMS: %,d - %,d/s \n",threadId,i,i/(soFar/1000));
                        } catch ( Exception e ) {}
                    }
                    queue.add(str1);
                    String key = getKey(threadId,i);
                    map.put(key, str1);
                    mmap.put(key, str1);
                    mmap.put(key, str2);
                    
                    if ( i > gap ) {
                        long j = i-gap;
                        if ( queue.poll() == null ) {
                            System.err.println("Nothing from queue");
                        }
                        key = getKey(threadId, j);
                        if ( map.remove(key) == null ) {
                            System.err.println("Can't find key in map");
                        }
                        if ( Math.random() > 0.5 ) {
                            if ( mmap.remove(key).isEmpty() ) {
                                System.err.println("Nothing returned from mmap");
                            }
                        } else {
                            if ( ! mmap.remove(key, str1) ) {
                                System.err.println("Can't find key in mmap");
                            }
                            if ( ! mmap.remove(key, str2) ) {
                                System.err.println("Can't find key in mmap");
                            }
                        }
                        
                    }
                    
                }
            }
        };
    }

    private static String getKey(long threadId, long i) {
        String k = "key-"+threadId+'-'+i;
        k = k.hashCode() +'-'+ k;
        k+= 'k'+k.hashCode();
        return k;
    }

    private static String getLongString(int i, int j) {
        StringBuilder b = new StringBuilder();
        for (;i<j;i++) {
            b.append(i % 10);
        }
        return b.toString();
    }
}