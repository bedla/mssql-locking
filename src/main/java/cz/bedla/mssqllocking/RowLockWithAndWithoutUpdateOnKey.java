package cz.bedla.mssqllocking;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.StopWatch;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.removeEnd;
import static org.apache.commons.lang3.StringUtils.removeStart;
import static org.apache.commons.lang3.StringUtils.trimToNull;

@Component
@Profile("row-lock-with-and-without-update-on-key")
class RowLockWithAndWithoutUpdateOnKey implements InitializingBean {
    private final DataProcessor dataProcessor;

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    private final ExecutorService executorService;

    RowLockWithAndWithoutUpdateOnKey(DataProcessor dataProcessor, JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.dataProcessor = dataProcessor;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.executorService = Executors.newFixedThreadPool(20);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        updateTableRows("foo.FOO_LOCK_TABLE_NO_PK");
        println("=========================================================");
        updateTableRows("foo.FOO_LOCK_TABLE_PK", "PK_FOO_LOCK_TABLE_PK");
    }

    private void updateTableRows(String tableName) {
        updateTableRows(tableName, null);
    }

    private void updateTableRows(String tableName, String primaryKeyName) {
        var count = 10;
        var thresholdId = (int) (count * 0.6);
        println("count=" + count);
        println("thresholdId=" + thresholdId);

        dataProcessor.truncateTable(tableName);
        dataProcessor.insertRecordsFooLockTable(count, tableName);

        final var recordIds = findRecordIds(tableName, primaryKeyName);

        var stopWatchAllExecuted = new StopWatch();
        stopWatchAllExecuted.start();
        var latchAllExecuted = new CountDownLatch(3);
        var latchWait2ndToStart = new CountDownLatch(1);

        executorService.submit(() -> {
            println("First) start");

            var stopwatch = new StopWatch();
            stopwatch.start();

            transactionTemplate.executeWithoutResult(status -> {
                println("First) tx isolation = " + dataProcessor.currentSessionIsolationLevel());
                println("First) Update IDs <= " + thresholdId);
                var updateCount = jdbcTemplate.update("""
                        UPDATE <<tableName>>
                        SET STATUS = ?
                        WHERE ID <= ?""".replace("<<tableName>>", tableName), "AAA", thresholdId);
                println("First) update-count = " + updateCount);

                println("First) row-locks = " + dataProcessor.rowLockCountForSession(executorService));
                dumpRowLocksWithData("First) locks before countDown & sleep", recordIds);

                latchWait2ndToStart.countDown();

                sleep("First)", 10);
                println("First) sleep ends");
                dumpRowLocksWithData("First) locks after sleep", recordIds);
            });
            stopwatch.stop();
            println("First) End, total seconds " + stopwatch.getTotalTimeSeconds());
            latchAllExecuted.countDown();
        });


        var latchWaitDumpLocksFromBothBeforeUpdate = new CountDownLatch(1);
        var latchWaitDumpLocksFromBothAfterUpdate = new CountDownLatch(1);

        executorService.submit(() -> {
            println("Dump all locks) start");
            var stopwatch = new StopWatch();
            stopwatch.start();

            await("Dump all locks) Wait until we are before second UPDATE", latchWaitDumpLocksFromBothBeforeUpdate);
            sleep("Dump all locks) Let SQL to locks", 2);

            transactionTemplate.executeWithoutResult(status -> {
                println("Dump all locks) tx isolation = " + dataProcessor.currentSessionIsolationLevel());
                dumpLocks("Dump all locks)", recordIds);
            });

            latchWaitDumpLocksFromBothAfterUpdate.countDown();

            println("Dump all locks) end");
            stopwatch.stop();
            println("Dump all locks) total seconds " + stopwatch.getTotalTimeSeconds());
            latchAllExecuted.countDown();
        });

        executorService.submit(() -> {
            println("Second) start");
            await("Second)", latchWait2ndToStart);

            var stopwatch = new StopWatch();
            stopwatch.start();

            transactionTemplate.executeWithoutResult(status -> {
                println("Second) tx isolation = " + dataProcessor.currentSessionIsolationLevel());

                var upperId = (int) (thresholdId + (count * 0.10));
                println("Second) Update IDs > " + upperId);

                latchWaitDumpLocksFromBothBeforeUpdate.countDown();
                var updateCount = jdbcTemplate.update("""
                        UPDATE <<tableName>>
                        SET STATUS = ?
                        WHERE ID > ?""".replace("<<tableName>>", tableName), "BBB", upperId);
                println("Second) update-count = " + updateCount);

                await("Second) after dumped all locks", latchWaitDumpLocksFromBothAfterUpdate);

                println("Second) row-locks = " + dataProcessor.rowLockCountForSession(executorService));
                dumpRowLocksWithData("Second) locks after UPDATE before commit", recordIds);
            });

            sleep("Second)", 10);
            println("Second) sleep ends");

            stopwatch.stop();
            println("Second) End, total seconds " + stopwatch.getTotalTimeSeconds());
            latchAllExecuted.countDown();
        });

        await("Wait all threads finishes work", latchAllExecuted);

        transactionTemplate.executeWithoutResult(status -> {
            var rows = jdbcTemplate.queryForList("""
                    SELECT *
                    FROM <<tableName>>
                    """.replace("<<tableName>>", tableName));
            println("final records");
            for (Map<String, Object> row : rows) {
                println(row.toString());
            }
        });

        stopWatchAllExecuted.stop();
        println("All threads finished work at " + stopWatchAllExecuted.getTotalTimeSeconds());
    }

    private void dumpLocks(String message, RecordIds recordIds) {
        println(message);
        var locks = dataProcessor.locksAllSessions(executorService).stream()
                .map(RowLockWithAndWithoutUpdateOnKey::normalizeResourceDescription)
                .map(row -> mapRowRecord(recordIds, row))
                .sorted(resourceTypeIdComparator())
                .toList();

        println(message + " count=" + locks.size());
        for (Map<String, Object> row : locks) {
            System.out.println(row);
        }
    }

    private void dumpRowLocksWithData(String message, RecordIds recordIds) {
        println(message);
        var currentSessionId = dataProcessor.currentSessionId();
        println(message + " @@spid=" + currentSessionId);

        var lockedRows = dataProcessor.locksAllSessions(executorService).stream()
                .filter(row -> ((Number) row.get("request_session_id")).intValue() == currentSessionId)
                .map(RowLockWithAndWithoutUpdateOnKey::normalizeResourceDescription)
                .map(row -> mapRowRecord(recordIds, row))
                .sorted(resourceTypeIdComparator())
                .toList();

        if (lockedRows.isEmpty()) {
            println(message + " no locks");
        } else {
            println(message + " count=" + lockedRows.size());
            for (Map<String, Object> row : lockedRows) {
                System.out.println(row);
            }
        }
    }

    private static Comparator<Map<String, Object>> resourceTypeIdComparator() {
        return Comparator
                .comparing((Map<String, Object> it) -> String.valueOf(it.get("resource_type")))
                .thenComparing(it -> {
                    var value = (Number) it.get("ID");
                    return value != null ? value.longValue() : 0;
                })
                .reversed();
    }

    private static LinkedHashMap<String, Object> mapRowRecord(RecordIds recordIds, Map<String, Object> row) {
        var newRow = new LinkedHashMap<String, Object>();
        var key = String.valueOf(row.get("resource_description"));
        newRow.put("resource_type", row.get("resource_type"));
        newRow.put("owner", row.get("owner"));
        newRow.put("request_mode", row.get("request_mode"));
        newRow.put("request_status", row.get("request_status"));
        newRow.put("resource_associated_entity_id", row.get("resource_associated_entity_id"));
        newRow.put("request_session_id", row.get("request_session_id"));
        newRow.put("resource_description", key);

        var rowByRowId = recordIds.rowIdRecords.get(key);
        if (rowByRowId != null) {
            newRow.put("ID", rowByRowId.get("ID"));
            newRow.put("STATUS", rowByRowId.get("STATUS"));
            newRow.put("EXECUTION_START", rowByRowId.get("EXECUTION_START"));
            newRow.put("EXECUTION_END", rowByRowId.get("EXECUTION_END"));
        }

        var rowByPkId = recordIds.pkIdRecords.get(key);
        if (rowByPkId != null) {
            newRow.put("ID", rowByPkId.get("ID"));
            newRow.put("STATUS", rowByPkId.get("STATUS"));
            newRow.put("EXECUTION_START", rowByPkId.get("EXECUTION_START"));
            newRow.put("EXECUTION_END", rowByPkId.get("EXECUTION_END"));
        }

        return newRow;
    }

    private static Map<String, Object> normalizeResourceDescription(Map<String, Object> row) {
        var resourceDescription = trimToNull(String.valueOf(row.get("resource_description")));
        row.put("resource_description", removeParentheses(resourceDescription));
        return row;
    }

    private RecordIds findRecordIds(String tableName, String primaryKeyName) {
        return transactionTemplate.execute(status ->
                new RecordIds(
                        pkIdRecords(tableName, primaryKeyName),
                        rowIdRecords(tableName)
                ));
    }

    private Map<String, Map<String, Object>> pkIdRecords(String tableName, String primaryKeyName) {
        return primaryKeyName == null
                ? new HashMap<>()
                : jdbcTemplate.queryForList("""
                        SELECT %%lockres%% KeyId, *
                        FROM <<tableName>> WITH (index (<<pkName>>), NOLOCK)"""
                        .replace("<<tableName>>", tableName)
                        .replace("<<pkName>>", primaryKeyName))
                .stream()
                .map(row -> {
                    var filePageSlot = removeParentheses(String.valueOf(row.get("KeyId")));
                    row.put("KeyId", filePageSlot);
                    return row;
                })
                .collect(Collectors.toMap(it -> String.valueOf(it.get("KeyId")), Function.identity()));
    }

    private Map<String, Map<String, Object>> rowIdRecords(String tableName) {
        return jdbcTemplate.queryForList("""
                        SELECT %%physloc%%                          AS [%%physloc%%],
                               sys.fn_PhysLocFormatter(%%physloc%%) AS [File:Page:Slot],
                               *
                        FROM <<tableName>>(NOLOCK)
                        """.replace("<<tableName>>", tableName)).stream()
                .map(row -> {
                    var filePageSlot = removeParentheses(String.valueOf(row.get("File:Page:Slot")));
                    row.put("File:Page:Slot", filePageSlot);
                    return row;
                })
                .collect(Collectors.toMap(it -> String.valueOf(it.get("File:Page:Slot")), Function.identity()));
    }

    private static void await(String why, CountDownLatch latch) {
        try {
            println(why + " latch.await");
            if (!latch.await(1, TimeUnit.MINUTES)) {
                println(why + ": ERROR not all actions finished");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ExceptionUtils.rethrow(e);
        }
    }

    private static void println(String message) {
        System.out.println(Thread.currentThread() + "\t" + message);
    }

    private static void sleep(String why, int howLong) {
        try {
            println(why + " Sleep for " + howLong + " s");
            TimeUnit.SECONDS.sleep(howLong);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ExceptionUtils.rethrow(e);
        }
    }

    private static String removeParentheses(String value) {
        return value == null ? null : removeEnd(removeStart(value, "("), ")");
    }

    record RecordIds(
            Map<String, Map<String, Object>> pkIdRecords,
            Map<String, Map<String, Object>> rowIdRecords
    ) {
    }
}
