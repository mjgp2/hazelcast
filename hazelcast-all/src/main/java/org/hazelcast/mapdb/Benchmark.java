package org.hazelcast.mapdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.MultiMapConfig.ValueCollectionType;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;

public class Benchmark {

	static AtomicLong threadIdGen = new AtomicLong();
	static AtomicLong clientThreadIdGen = new AtomicLong();
	static AtomicLong idGen = new AtomicLong();
	static AtomicLong removed = new AtomicLong();
	static AtomicLong lost = new AtomicLong();
	static AtomicLong cycles = new AtomicLong();
	static LinkedBlockingDeque<Long> idQueue = new LinkedBlockingDeque<Long>(
			10000);
	static List<Address> members = new ArrayList<Address>();

	static String str1 = getLongString(0, 5000);
	static String str2 = getLongString(1, 5001);
	static List<String> mmapCollection = Arrays.asList(str1,str2);

	static int clients = 8;
	static int servers = 4;
	static AtomicInteger count = new AtomicInteger();
	static ReentrantLock lock = new ReentrantLock();


	static boolean initialized;
	static long startTime;

	static long lastCycle;
	
	
	
	
	public static void main(String[] args) throws InterruptedException {
		for (int thread = 1; thread <= servers; thread++) {
			startNodeAndThread(false);
		}

		while (count.get() != servers) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		Thread.sleep(10000);
		
		startTime = System.currentTimeMillis();
		initialized = true;
		
		for (int thread = 1; thread <= clients; thread++) {
			startClient();
		}
		
		while (true ) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				return;
			}
			long r = removed.get();
			if (r > 0 ) {
				long soFar = System.currentTimeMillis() - startTime;
				try {
					System.out.println("CYCLES: "+cycles.get()+"  LOST: "+lost.get()+" ("+((float)lost.get()/removed.get())+"%)  OPS: "+( removed.get() / (soFar / 1000) ) +"/s");
				} catch (Exception e) {
				}
			}
		}

	}

	private static void startNodeAndThread(boolean migrate) {
		long threadId = threadIdGen.incrementAndGet();
		Thread thread = new Thread(makeRunnable(threadId, migrate), "server-"
				+ threadId);
		thread.start();
	}

	private static void startClient() {
		long threadId = clientThreadIdGen.incrementAndGet();
		Thread thread = new Thread(makeClientRunnable(threadId), "client-"
				+ threadId);
		thread.start();
	}

	private static Runnable makeClientRunnable(final long id) {
		return new Runnable() {

			private HazelcastInstance instance;
			private IQueue<Object> queue;
			private IMap<Object, Object> map;
			private MultiMap<Object, Object> mmap;

			public void run() {
				ClientConfig clientConfig = new ClientConfig();

				// hzmember =
				// Configurer.getProperty("hazelcast.client.hzmember");
				// if(! StringUtil.isNullOrEmpty(hzmember)) {
				// log.info("Using manual address for hzmember: {}", hzmember);
				// clientConfig.getNetworkConfig().addAddress(hzmember);
				// }

				// hazelcast.client.smartRouting=true
				// hazelcast.client.redoOperation=true
				// hazelcast.client.connectionTimeout=10000
				// hazelcast.client.connectionAttemptLimit=100
				// hazelcast.client.connectionAttemptPeriod=2000

				clientConfig.setLoadBalancer(new LoadBalancer() {

					public Member next() {
						
						synchronized (members) {
							if ( members.isEmpty() ) {
								return null;
							}
							return new MemberImpl(members.get((int) (Math.random() * members.size())), false);
						}
					}

					public void init(Cluster cluster, ClientConfig config) {
					}
				});
				clientConfig.getNetworkConfig().setSmartRouting(false);
				clientConfig.getNetworkConfig().setRedoOperation(true);
				clientConfig.getNetworkConfig().setConnectionTimeout(10000);
				clientConfig.getNetworkConfig().setConnectionAttemptLimit(100);
				clientConfig.getNetworkConfig()
						.setConnectionAttemptPeriod(2000);

				instance = HazelcastClient.newHazelcastClient(clientConfig);

				getProxies();
				pullFromProxies();
			}

			private void pullFromProxies() {
				for (;;) {
					try {
						Long j;
						try {
							j = Benchmark.idQueue.poll(10, TimeUnit.SECONDS);
						} catch (InterruptedException e) {
							e.printStackTrace();
							continue;
						}
						if (j == null) {
							System.err.println("No id fetched");
							continue;
						}
						
						removed.incrementAndGet();
						
						check(j);

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			private void check(Long j) throws InterruptedException {
				String key = getKey(j);
				while (true) {
					try {
						Object removedStr1 = map.remove(key);
						if ( ! str1.equals(removedStr1) ) {
							System.err
									.println("!!!!!!!!!!!!!!!!!!!! Can't find key in map");
							lost.incrementAndGet();
						}
						break;
					} catch (HazelcastInstanceNotActiveException e) {
						// retry
						Thread.sleep(1000);
					}
				}
				if (Math.random() > 0.5) {

					while (true) {
						try {
							Collection<Object> removed = mmap.remove(key);
							
							// TODO: sometimes the order of lists becomes jumbled!!
							if (removed == null || removed.size() != 2 || ! new HashSet<String>(mmapCollection).equals(new HashSet<Object>(removed))) {
								System.err
										.println("!!!!!!!!!!!!!!!!!!!! Wrong data returned from mmap");
								lost.incrementAndGet();
							}
							break;
						} catch (HazelcastInstanceNotActiveException e) {
							// retry
							Thread.sleep(1000);
						}
					}

				} else {

					while (true) {
						try {
							if (!mmap.remove(key, str1)) {
								System.err
										.println("!!!!!!!!!!!!!!!!!!!! Can't find key in mmap");
								lost.incrementAndGet();
							}
							break;
						} catch (HazelcastInstanceNotActiveException e) {
							// retry
							Thread.sleep(1000);
						}
					}

					while (true) {
						try {
							if (!mmap.remove(key, str2)) {
								System.err
										.println("!!!!!!!!!!!!!!!!!!!! Can't find key in mmap");
								lost.incrementAndGet();
							}
							break;
						} catch (HazelcastInstanceNotActiveException e) {
							// retry
							Thread.sleep(1000);
						}
					}

				}

				// this is inefficient but the only way to check if it's on the
				// queue
				for (int i = 0; i < 11; i++) {
					while (true) {
						try {
							if (queue.remove(key)) {
								return;
							}
							break;
						} catch (HazelcastInstanceNotActiveException e) {
							// retry
							Thread.sleep(1000);
						}
					}
					System.err.println("PAUSING! Not on queue");
					Thread.sleep(1000);
				}
				System.err.println("!!!!!!!!!!!!!!!!!!!! Nothing from queue");
				lost.incrementAndGet();
			}

			private void getProxies() {
				queue = instance.getQueue("queue");
				map = instance.getMap("map");
				mmap = instance.getMultiMap("mmap");
			}
		};
	}


	private static Runnable makeRunnable(final long threadId,
			final boolean migrate) {
		return new Runnable() {

			public void run() {
				Config cfg = new Config();
				cfg.addMultiMapConfig(new MultiMapConfig().setName("mmap").setValueCollectionType(ValueCollectionType.LIST));

				HazelcastInstance instance = Hazelcast
						.newHazelcastInstance(cfg);
				final Address member = ((MemberImpl) instance.getCluster().getLocalMember()).getAddress();
				instance.getLifecycleService().addLifecycleListener(
						new LifecycleListener() {

							public void stateChanged(LifecycleEvent event) {
								switch (event.getState()) {
								case CLIENT_CONNECTED:
									break;
								case CLIENT_DISCONNECTED:
									break;
								case MERGED:
									break;
								case MERGING:
									break;
								case SHUTDOWN:
									startNodeAndThread(true);
									break;
								case SHUTTING_DOWN:
									synchronized (members) {
										if ( ! members.remove(member) ) {
											System.err.println("!!!!! CANNOT FIND MEMBER");
										}
									}
									break;
								case STARTED:
									break;
								case STARTING:
									break;
								default:
									break;

								}
							}
						});

				// give it a chance to connect/migrate before we kill another
				// node
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}

				count.incrementAndGet();

				synchronized (members) {
					members.add(member);
				}

				IQueue<Object> queue = instance.getQueue("queue");
				IMap<Object, Object> map = instance.getMap("map");
				MultiMap<Object, Object> mmap = instance.getMultiMap("mmap");

				String key = "";
				try {
					long it = 0;
					for (;; it++) {

						long keyId = idGen.incrementAndGet();
						key = getKey(keyId);

						queue.add(key);
						map.put(key, str1);
						mmap.put(key, str1);
						mmap.put(key, str2);

						Benchmark.idQueue.offerLast(keyId, Long.MAX_VALUE,
								TimeUnit.MILLISECONDS);

						if ( initialized && System.currentTimeMillis() - lastCycle > 30000 && count.compareAndSet(servers, servers - 1)) {
							lastCycle = System.currentTimeMillis();
							System.err.println("Cycling...");
							cycles.incrementAndGet();
							instance.shutdown();
							return;
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					try {
						queue.remove(key);
						map.remove(key, str1);
						mmap.remove(key, str1);
						mmap.remove(key, str2);
					} finally {
						System.err.println("!!!!!! Forcing shutdown...");
						instance.shutdown();
					}
				}
			}
		};
	}

	private static String getKey(long i) {
		// hash it to make it more random
		String k = "key-" + i;
		k = k.hashCode() + '-' + k + '-' + k.hashCode();
		return k;
	}

	private static String getLongString(int i, int j) {
		StringBuilder b = new StringBuilder();
		for (; i < j; i++) {
			b.append(i % 10);
		}
		return b.toString();
	}
}