package headwater.data;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class CassandraIO implements IO {

    private Keyspace keyspace;
    private final ColumnFamily<byte[], byte[]> columnFamily;
    private final AstyanaxContext.Builder builder;
    
    private final Timer putTimer = makeTimer(CassandraIO.class, "put", "cassandra");
    private final Timer getTimer = makeTimer(CassandraIO.class, "get", "cassandra");
    private final Timer visitAllTimer = makeTimer(CassandraIO.class, "visit", "cassandra");

    public CassandraIO(String host, int port, String keyspace, String columnFamily) {
        builder = new AstyanaxContext.Builder()
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl()
                                .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                .setCqlVersion("3.0.0")
                                .setTargetCassandraVersion("1.2")
                                .setDiscoveryType(NodeDiscoveryType.NONE)
                                .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                )
                .forKeyspace(keyspace)
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(String.format("%s:%d", host, port))
                        .setPort(port)
                        .setMaxConnsPerHost(10)
                        .setSeeds(String.format("%s:%d", host, port))
                );
        
        AstyanaxContext<Keyspace> context = builder.buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
        this.keyspace = context.getEntity();
        this.columnFamily = ColumnFamily.newColumnFamily(columnFamily, BytesArraySerializer.get(), BytesArraySerializer.get(), BytesArraySerializer.get());
    }
    
    public void put(byte[] key, byte[] col, byte[] value) throws Exception {
        TimerContext ctx = putTimer.time();
        try {
            keyspace.prepareColumnMutation(columnFamily, key, col).putValue(value, null).execute();
        } finally {
            ctx.stop();
        }
    }
    
    public byte[] get(byte[] key, byte[] col) throws Exception {
        TimerContext ctx = getTimer.time();
        try {
            return keyspace.prepareQuery(columnFamily)
                    .getKey(key)
                    .getColumn(col)
                    .execute().getResult().getByteArrayValue();
        } finally {
            ctx.stop();
        }
    }
    
    // iterate over all columns, paging through data in a row.
    public void visitAllColumns(byte[] key, int pageSize, ColumnObserver observer) throws Exception {
        
        TimerContext ctx = visitAllTimer.time();
        try {
            RowQuery<byte[], byte[]> query = keyspace
                    .prepareQuery(columnFamily)
                    .getKey(key)
                    .autoPaginate(true)
                    .withColumnRange(new RangeBuilder().setLimit(pageSize).build());
            
            ColumnList<byte[]> columnList;
            while (!(columnList = query.execute().getResult()).isEmpty()) {
                for (Column<byte[]> col : columnList) {
                    observer.observe(key, col.getName(), col.getByteArrayValue());
                }
            }
        } finally {
            ctx.stop();
        }
    }

    public void del(byte[] key, byte[] col) throws Exception {
        keyspace.prepareColumnMutation(columnFamily, key, col).deleteColumn().execute();
    }
    
    private static final Map<MetricName, Timer> metrics = new HashMap<MetricName, Timer>();
    private static Timer makeTimer(Class cls, String name, String scope) {
        MetricName metricName = new MetricName(cls, name, scope);
        if (metrics.containsKey(metricName))
            return metrics.get(metricName);
        else {
            Timer timer = Metrics.newTimer(metricName, TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
            metrics.put(metricName, timer);
            return timer;
        }
    }
}
