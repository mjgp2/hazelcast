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

public class Benchmark {

	static AtomicLong threadIdGen = new AtomicLong();
	static AtomicLong clientThreadIdGen = new AtomicLong();
	static AtomicLong idGen = new AtomicLong();
	static LinkedBlockingDeque<Long> idQueue = new LinkedBlockingDeque<Long>(
			10000);
	static List<Member> members = new ArrayList<Member>();

	static String str1 = getLongString(0, 5000);
	static String str2 = getLongString(1, 5001);
	static List<String> mmapCollection = Arrays.asList(str1,str2);
	
	static int threads = 3;
	static AtomicInteger count = new AtomicInteger();
	static ReentrantLock lock = new ReentrantLock();

	public static void main(String[] args) {
		for (int thread = 1; thread <= threads; thread++) {
			startNodeAndThread(false);
		}

		while (count.get() != threads) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		startClient();

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

			private long threadId = id;
			private HazelcastInstance instance;
			private IQueue<Object> queue;
			private IMap<Object, Object> map;
			private MultiMap<Object, Object> mmap;
			private int removed = 0;
			private final long startTime = System.currentTimeMillis();

			private long lost;

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
							return members.get((int) (Math.random() * members
									.size()));
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
				for (;; removed++) {
					try {
						if (removed > 0 && removed % 100 == 0) {
							long soFar = System.currentTimeMillis() - startTime;
							try {
								System.out
										.printf("%d CLIENT %d/%d, QUEUE: %,d, ITEMS: %,d, LOST VALUES: %,d, %,d/s \n",
												System.currentTimeMillis(), threadId,instance.getCluster().getMembers().size(), idQueue.size(), removed, lost,
												removed / (soFar / 1000));
							} catch (Exception e) {
							}
						}
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
						if ( ! map.remove(key).equals(str1) ) {
							System.err
									.println("!!!!!!!!!!!!!!!!!!!! Can't find key in map");
							lost++;
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
							if (removed == null || removed.size() != 2 || ! mmapCollection.equals(removed)) {
								System.err
										.println("!!!!!!!!!!!!!!!!!!!! Wrong data returned from mmap");
								lost++;
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
								lost++;
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
								lost++;
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
				lost++;
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

				HazelcastInstance instance = Hazelcast
						.newHazelcastInstance(cfg);
				final Member member = instance.getCluster().getLocalMember();
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
										members.remove(member);
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

				final long startTime = System.currentTimeMillis();

				String key = "";
				try {
					long it = 0;
					for (;; it++) {

						long keyId = idGen.incrementAndGet();
						key = getKey(keyId);

						if (it > 0 && it % 1000 == 0) {
							long soFar = System.currentTimeMillis() - startTime;
							try {
								System.out.printf(
										"SERVER %d, ITEMS: %,d - %,d/s \n",
										threadId, it, it / (soFar / 1000));
							} catch (Exception e) {
							}
						}

						queue.add(key);
						map.put(key, str1);
						mmap.put(key, str1);
						mmap.put(key, str2);

						Benchmark.idQueue.offerLast(keyId, Long.MAX_VALUE,
								TimeUnit.MILLISECONDS);

						if (it > 10000 && Math.random() > 0.999 && count.compareAndSet(threads, threads - 1)) {
							System.err.println("Cycling...");
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