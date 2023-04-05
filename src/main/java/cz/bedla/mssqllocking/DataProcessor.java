package cz.bedla.mssqllocking;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class DataProcessor {
    protected static final String LOCKS_SQL = """
            WITH transactionLocks AS (SELECT resource_type,
                                             resource_associated_entity_id,
                                             request_status,
                                             request_mode,
                                             request_session_id,
                                             resource_description
                                      FROM sys.dm_tran_locks
                                      WHERE resource_type <> 'DATABASE'
                                        %s),
                 ridOwners AS (SELECT hobt_id,
                                      object_name(object_id) ownerName
                               FROM sys.partitions
                               WHERE hobt_id IN (SELECT resource_associated_entity_id FROM transactionLocks)),
                 objects AS (SELECT object_id,
                                    name COLLATE SQL_Latin1_General_CP1_CI_AS      AS name,
                                    type COLLATE SQL_Latin1_General_CP1_CI_AS      AS type,
                                    type_desc COLLATE SQL_Latin1_General_CP1_CI_AS AS type_desc
                             FROM sys.objects
                             WHERE object_id IN (SELECT resource_associated_entity_id FROM transactionLocks)),
                 myLocks AS (SELECT resource_type,
                                    CASE resource_type
                                        WHEN 'RID' THEN (SELECT ownerName
                                                         FROM ridOwners
                                                         WHERE hobt_id = resource_associated_entity_id)
                                        WHEN 'OBJECT' THEN (SELECT CONCAT(name, ' : ', type, ': ', type_desc)
                                                            FROM objects
                                                            WHERE object_id = resource_associated_entity_id)
                                        ELSE '<n/a>'
                                        END AS owner,
                                    request_mode,
                                    request_status,
                                    resource_associated_entity_id,
                                    request_session_id,
                                    resource_description
                             FROM transactionLocks)
                             """;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    public DataProcessor(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    public void insertRecordsFooLockTable(int count, String tableName) {
        transactionTemplate.executeWithoutResult(status -> {
            System.out.println("Start loading " + count + " records");
            var batches = new ArrayList<Object[]>();
            for (int i = 0; i < count; i++) {
                var row = new Object[]{
                        "XXX",
                        Date.from(ZonedDateTime.now().toInstant()),
                        RandomUtils.nextBoolean() ? Date.from(ZonedDateTime.now().toInstant()) : null};
                batches.add(row);
            }
            jdbcTemplate.batchUpdate("INSERT INTO " + tableName + "(STATUS, EXECUTION_START, EXECUTION_END) VALUES( ?, ?, ?)", batches);
            System.out.println("Loading records finished");
        });
    }

    public void truncateTable(String tableName) {
        transactionTemplate.executeWithoutResult(status -> jdbcTemplate.update("TRUNCATE TABLE " + tableName));
    }

    public int rowLockCountForSession(ExecutorService executorService) {
        return rowLockCount(executorService, sessionLocksSql());
    }

    public int rowLockCountAllSessions(ExecutorService executorService) {
        return rowLockCount(executorService, allLocksSql());
    }

    public int tableLockCountAllSessions(ExecutorService executorService) {
        return tableLockCount(executorService, allLocksSql());
    }

    private int rowLockCount(ExecutorService executorService, String sqlWith) {
        return objectLockCount(executorService, sqlWith, "RID");
    }

    private int tableLockCount(ExecutorService executorService, String sqlWith) {
        return objectLockCount(executorService, sqlWith, "OBJECT");
    }

    private int objectLockCount(ExecutorService executorService, String sqlWith, String resourceType) {
        var countFuture = executorService.submit(() ->
                transactionTemplate.execute(status -> {
                    var count = jdbcTemplate.queryForObject(sqlWith + """
                            SELECT COUNT(*)
                            FROM myLocks
                            WHERE resource_type = ?  
                                  AND request_mode = 'X'
                                  AND request_status = 'GRANT'                
                            """, Number.class, resourceType);
                    return count.intValue();
                }));

        try {
            return countFuture.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ExceptionUtils.rethrow(e);
        } catch (ExecutionException | TimeoutException e) {
            return ExceptionUtils.rethrow(e);
        }
    }

    public List<Map<String, Object>> rowLocksPerSession(ExecutorService executorService) {
        return locks(executorService, sessionLocksSql());
    }

    public List<Map<String, Object>> locksAllSessions(ExecutorService executorService) {
        return locks(executorService, allLocksSql());
    }

    private List<Map<String, Object>> locks(ExecutorService executorService, String sqlWith) {
        var countFuture = executorService.submit(() ->
                transactionTemplate.execute(status -> {
                    var records = jdbcTemplate.queryForList(sqlWith + """
                            SELECT *
                            FROM myLocks                         
                            """);
                    return records;
                }));

        try {
            return countFuture.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ExceptionUtils.rethrow(e);
        } catch (ExecutionException | TimeoutException e) {
            return ExceptionUtils.rethrow(e);
        }
    }

    public int currentSessionId() {
        return jdbcTemplate.queryForObject("SELECT @@spid", Number.class).intValue();
    }

    public String currentSessionIsolationLevel() {
        return jdbcTemplate.queryForObject("""
                SELECT CASE transaction_isolation_level
                           WHEN 0 THEN 'Unspecified'
                           WHEN 1 THEN 'ReadUncommitted'
                           WHEN 2 THEN 'ReadCommitted'
                           WHEN 3 THEN 'Repeatable'
                           WHEN 4 THEN 'Serializable'
                           WHEN 5 THEN 'Snapshot' END AS TRANSACTION_ISOLATION_LEVEL
                FROM sys.dm_exec_sessions
                where session_id = @@SPID
                """, String.class);
    }

    private static String sessionLocksSql() {
        return String.format(LOCKS_SQL, "AND request_session_id = @@spid");
    }

    private static String allLocksSql() {
        return String.format(LOCKS_SQL, "");
    }
}
