package cz.bedla.mssqllocking;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.StopWatch;

import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@Profile({
        "tx-wait",
        "tx-wait-mvcc"
})
class TxWaitMVCC implements InitializingBean {
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    TxWaitMVCC(
            JdbcTemplate jdbcTemplate,
            TransactionTemplate transactionTemplate
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        // https://stackoverflow.com/a/7887033/535560
        dropTable("TestTable");
        dropTable("TestTable2");
        createTable("TestTable");
        createTable("TestTable2");
        fillTable("TestTable");
        fillTable("TestTable2");

        var stopwatchT1 = new StopWatch("Tx1");
        var stopwatchT2 = new StopWatch("Tx2");

        var executorService = Executors.newFixedThreadPool(5);

        var latch = new CountDownLatch(2);

        executorService.submit(() -> {
            log("Begin Tx1");

            stopwatchT1.start();
            transactionTemplate.executeWithoutResult(status -> {
                jdbcTemplate.update("""
                        UPDATE TestTable
                         SET Val='X'
                        WHERE Val='A'
                        WAITFOR DELAY '00:00:15'""");
            });
            stopwatchT1.stop();

            log("End Tx1");

            latch.countDown();
        });

        executorService.submit(() -> {
            log("Sleep Tx2");
            sleep();

            log("Begin Tx2");

            stopwatchT2.start();
            transactionTemplate.executeWithoutResult(status -> {
                jdbcTemplate.queryForList("SELECT * FROM TestTable");
            });
            stopwatchT2.stop();

            log("End Tx2");

            latch.countDown();
        });

        var counted = latch.await(20, TimeUnit.SECONDS);
        if (!counted) {
            throw new IllegalStateException("Waiting time elapsed before the count reached zero");
        }

        log("Tx1 time=" + stopwatchT1.getTotalTimeSeconds());
        log("Tx2 time=" + stopwatchT2.getTotalTimeSeconds());
    }

    private void fillTable(String tableName) {
        transactionTemplate.executeWithoutResult(status -> {
            log("Fill table with data:" + tableName);
            jdbcTemplate.update("""
                    INSERT INTO %s(ID, Val)
                    VALUES (1, 'A'),
                           (2, 'B'),
                           (3, 'C')""".formatted(tableName));
        });
    }

    private void dropTable(String tableName) {
        transactionTemplate.executeWithoutResult(status -> {
            log("Drop table: " + tableName);
            jdbcTemplate.update("""
                    IF OBJECT_ID('dbo.%s', 'U') IS NOT NULL
                       DROP TABLE dbo.%s;""".formatted(tableName, tableName));
        });
    }

    private void createTable(String tableName) {
        transactionTemplate.executeWithoutResult(status -> {
            log("Create table: " + tableName);
            jdbcTemplate.update("""
                    CREATE TABLE %s
                    (
                        ID  INT,
                        Val CHAR(1)
                    )""".formatted(tableName));
        });
    }

    private static void sleep() {
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void log(String msg) {
        System.out.println(LocalDateTime.now() + " [" + Thread.currentThread().getName() + "]> " + msg);
    }
}
