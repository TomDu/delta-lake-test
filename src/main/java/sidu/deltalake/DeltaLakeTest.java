package sidu.deltalake;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import io.delta.standalone.*;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.expressions.EqualTo;
import io.delta.standalone.expressions.Literal;
import io.delta.standalone.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class DeltaLakeTest {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaLakeTest.class);

    public static void main(String[] args) {
        int threadCount = 1;
        int logFilesPerThread = 3;
        if (args != null && args.length > 1) {
            threadCount = Integer.parseInt(args[0]);
            logFilesPerThread = Integer.parseInt(args[1]);
        }

        new DeltaLakeTest().run(threadCount, logFilesPerThread);
    }

    public void run(int threadCount, int logFilesPerThread) {
        DeltaLakeConfig deltaLakeConfig = new DeltaLakeConfig();
        DeltaLog log = DeltaLog.forTable(deltaLakeConfig.getHadoopConf(), deltaLakeConfig.getDeltaTablePath());

/*
        Snapshot latestSnapshot = log.update();
        StructType schema = latestSnapshot.getMetadata().getSchema();
        LOG.info("Schema: {}", new Gson().toJson(schema));
*/

        LOG.info("--- START ---");
        Stopwatch sw = Stopwatch.createStarted();

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount, new ThreadFactoryBuilder().setNameFormat("Metadata-Update-Worker-%d").build());
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; ++i) {
            int finalI = i + 1;
            futures.add(executorService.submit(() -> writeLog(log, finalI, logFilesPerThread)));
        }

        for (int i = 0; i < threadCount; ++i) {
            try {
                futures.get(i).get(10, TimeUnit.MINUTES);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOG.error("Error: ", e);
            }
        }

        executorService.shutdown();

        LOG.info("ThreadCount: {}; LogFilesPerThread: {}; Duration: {}; PerTxDurationMs: {}", threadCount, logFilesPerThread, sw.stop(), sw.elapsed(TimeUnit.MILLISECONDS) / ((long) threadCount * logFilesPerThread));
        LOG.info("--- END ---");
    }

    private void writeLog(DeltaLog log, int threadId, int logFilesPerThread) {
        Map<String, String> tags = new HashMap<>();
        tags.put("Foo", "Bar");

        Map<String, String> operationParameters = new HashMap<>();
        operationParameters.put("mode", "\"Append\"");

        LOG.info("Start update");
        for (int idx = 0; idx < logFilesPerThread; ++idx) {
            List<AddFile> addNewFiles = new ArrayList<>();
            String fileName = String.format("T%s-%s.snappy.parquet", threadId, idx);
            addNewFiles.add(new AddFile(fileName, new HashMap<>(), 1024, System.currentTimeMillis(), true, null, tags));
            List<Action> totalCommitFiles = new ArrayList<>(addNewFiles);
            try {
                OptimisticTransaction txn = log.startTransaction();
                txn.commit(totalCommitFiles, new Operation(Operation.Name.WRITE, operationParameters), "Zippy/1.0.0");
                LOG.info("Committed log for {}", fileName);
            } catch (Exception e) {
                LOG.error("Committed log for {} failed:", fileName, e);
            }
        }
        LOG.info("End update");
    }

    public void query() {
        DeltaLakeConfig deltaLakeConfig = new DeltaLakeConfig();
        DeltaLog log = DeltaLog.forTable(deltaLakeConfig.getHadoopConf(), deltaLakeConfig.getDeltaTablePath());

        Snapshot latestSnapshot = log.update();
        StructType schema = latestSnapshot.getMetadata().getSchema();

        OptimisticTransaction txn = log.startTransaction();

        assert schema != null;
        DeltaScan scan = txn.markFilesAsRead(
                new EqualTo(schema.column("id"), Literal.of(1))
        );

        CloseableIterator<AddFile> iter = scan.getFiles();
        Map<String, AddFile> addFileMap = new HashMap<>();
        while (iter.hasNext()) {
            AddFile addFile = iter.next();
            addFileMap.put(addFile.getPath(), addFile);
        }
        try {
            iter.close();
        } catch (IOException e) {
            LOG.error("Error:", e);
        }

        LOG.info(new Gson().toJson(addFileMap));
    }
}
