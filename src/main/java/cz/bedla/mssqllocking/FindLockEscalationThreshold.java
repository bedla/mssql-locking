package cz.bedla.mssqllocking;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@Profile("find-lock-escalation-threshold")
class FindLockEscalationThreshold implements InitializingBean {
    private final DataProcessor dataProcessor;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final ExecutorService executorService;

    FindLockEscalationThreshold(
            DataProcessor dataProcessor,
            JdbcTemplate jdbcTemplate,
            TransactionTemplate transactionTemplate
    ) {
        this.dataProcessor = dataProcessor;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.executorService = Executors.newFixedThreadPool(5);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        var count = 10_000;
//        var count = 5_000;

        dataProcessor.truncateTable("foo.FOO_LOCK_TABLE_NO_PK");
        dataProcessor.insertRecordsFooLockTable(count, "foo.FOO_LOCK_TABLE_NO_PK");

        System.out.println("lock escalation threshold = " + findLockEscalationThreshold(count));
    }

    private int findLockEscalationThreshold(int count) {
        var range = new Range(1, count);
        var iteration = 0;
        while (true) {
            iteration++;
            if (iteration > count) {
                throw new IllegalStateException("Wrong iteration count(current=" + iteration + ", max=" + count + "), range=" + range);
            }

            var lockCountLeft = findLocksCount(range.leftId);
            var lockCountRight = findLocksCount(range.rightId);

            System.out.print(range + " => locks (" + lockCountLeft + "," + lockCountRight + ")");


            var leftAndRightHaveRowLock = lockCountLeft.tableLockCount == 0
                    && lockCountRight.tableLockCount == 0;
            var leftHasRowLockRightHasTableLock = lockCountLeft.tableLockCount == 0
                    && lockCountRight.tableLockCount > 0;
            var leftHasTableLockRightHasRowLock = lockCountLeft.tableLockCount > 0
                    && lockCountRight.tableLockCount == 0; // impossible
            var leftAndRightHaveTableLock = lockCountLeft.tableLockCount > 0
                    && lockCountRight.tableLockCount > 0;

            if (range.rightId - range.leftId <= 1) {
                if (leftAndRightHaveTableLock) {
                    System.out.println(" (TABLE_LOCK, TABLE_LOCK)");
                    if (lockCountLeft.rowLockCount == 0 && lockCountRight.rowLockCount == 0) {
                        // behavior of MSSQL 2017, 2019
                        return Math.max(range.leftId, range.rightId);
                    } else {
                        // behavior of MSSQL 2022
                        return Math.max(lockCountLeft.rowLockCount, lockCountRight.rowLockCount);
                    }
                } else {
                    throw new IllegalStateException("Unable to find lock escalation threshold. Do you have MVCC enabled for your DB?");
                }
            }

            if (leftAndRightHaveRowLock) {
                System.out.println(" (ROW_LOCK, ROW_LOCK)");
                throw new IllegalStateException("Unable to find lock escalation, increase start count from " + count + " to bigger number");
            } else if (leftHasRowLockRightHasTableLock) {
                System.out.println(" (ROW_LOCK, TABLE_LOCK)");
                range = new Range(range.leftId + ((range.rightId - range.leftId) / 2), range.rightId);
           } else if (leftHasTableLockRightHasRowLock) {
                throw new IllegalStateException("Impossible to happen lockCountLeft=" + lockCountLeft + ", lockCountRight=" + lockCountRight + ", range=" + range);
            } else if (leftAndRightHaveTableLock) {
                System.out.println(" (TABLE_LOCK, TABLE_LOCK)");
                range = new Range(range.leftId - ((range.rightId - range.leftId) / 2), range.leftId);
            } else {
                throw new IllegalStateException("Impossible to happen lockCountLeft=" + lockCountLeft + ", lockCountRight=" + lockCountRight + ", range=" + range);
            }
        }
    }

    private Locks findLocksCount(int index) {
        return transactionTemplate.execute(status -> {
            jdbcTemplate.update("""
                    UPDATE foo.FOO_LOCK_TABLE_NO_PK
                    SET STATUS = ?
                    WHERE ID <= ?""", "XXX", index);
            var rowLockCount = dataProcessor.rowLockCountAllSessions(executorService);
            var tableLockCount = dataProcessor.tableLockCountAllSessions(executorService);
            return new Locks(rowLockCount, tableLockCount);
        });
    }

    record Locks(int rowLockCount, int tableLockCount) {
    }

    record Range(int leftId, int rightId) {
    }
}
