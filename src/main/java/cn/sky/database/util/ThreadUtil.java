package cn.sky.database.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * @author Sky
 * @date 2018/4/20 下午2:39
 */
public class ThreadUtil {

    private ThreadUtil() {

    }

    /**
     * get a specified size of the thread pool which can execute runnable or callable tasks asynchronously
     *
     * @param threadPoolSize
     * @return
     */
    public static final ExecutorService getExecutorService(int threadPoolSize) {
        ThreadPoolExecutor executor = ThreadPoolHolder.executorService;
        executor.setCorePoolSize(threadPoolSize);
        executor.setMaximumPoolSize(threadPoolSize);
        return executor;
    }

    /**
     * shutdown the thread pool
     */
    public static final void shutdown() {
        ThreadPoolHolder.shutdown();
    }

    private static final class ThreadPoolHolder {
        private static final ThreadPoolExecutor executorService = getInstance();

        private static final ThreadPoolExecutor getInstance() {
            ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("thread-pool-%d").build();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 1L, TimeUnit.SECONDS,
                    new LinkedBlockingDeque<>(1024), threadFactory);
            return executor;
        }

        private static void shutdown() {
            if (null != executorService) {
                executorService.shutdown();
            }
        }
    }
}
