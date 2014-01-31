package headwater.io;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
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
import headwater.Utils;
import headwater.bitmap.IBitmap;
import headwater.bitmap.MemoryBitmap2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CassandraBitmapIO implements IO<Long, IBitmap> {

    private Keyspace keyspace;
    private final ColumnFamily<byte[], byte[]> columnFamily;
    private final AstyanaxContext.Builder builder;
    
    private static final Timer putTimer = Utils.getMetricRegistry().timer(MetricRegistry.name(CassandraBitmapIO.class, "cassandra", "put"));
    private static final Timer getTimer = Utils.getMetricRegistry().timer(MetricRegistry.name(CassandraBitmapIO.class, "cassandra", "get"));
    private static final Timer bulkGetTimer = Utils.getMetricRegistry().timer(MetricRegistry.name(CassandraBitmapIO.class, "cassandra", "bulk-get"));
    private static final Histogram bulkGetHist = Utils.getMetricRegistry().histogram(MetricRegistry.name(CassandraBitmapIO.class, "cassandra", "bulk-get-counts"));
    private static final Timer visitAllTimer = Utils.getMetricRegistry().timer(MetricRegistry.name(CassandraBitmapIO.class, "cassandra", "visit"));
    private static final Timer flushTimer = Utils.getMetricRegistry().timer(MetricRegistry.name(CassandraBitmapIO.class, "cassandra", "flush"));
    private static final Timer batchTimer = Utils.getMetricRegistry().timer(MetricRegistry.name(CassandraBitmapIO.class, "cassandra", "batch"));
    

    public CassandraBitmapIO(String host, int port, String keyspace, String columnFamily) {
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
    
    public void put(byte[] key, Long col, IBitmap value) throws Exception {
        Timer.Context ctx = putTimer.time();
        try {
            keyspace.prepareColumnMutation(columnFamily, key, Utils.longToBytes(col)).putValue(value.toBytes(), null).execute();
        } finally {
            ctx.stop();
        }
    }
    
    public void flush(Map<byte[], Map<Long, IBitmap>> data) throws Exception {
        Timer.Context ctx = flushTimer.time();
        int colCount = 0, maxCols = 1024;
        MutationBatch batch = keyspace.prepareMutationBatch().lockCurrentTimestamp();
        for (byte[] key : data.keySet()) {
            
            if (colCount >= maxCols) {
                tryBatch(batch, colCount);
                batch = keyspace.prepareMutationBatch().lockCurrentTimestamp();
            }
            
            ColumnListMutation<byte[]> mutation = batch.withRow(columnFamily, key);
            for (Map.Entry<Long, IBitmap> entry : data.get(key).entrySet()) {
                mutation = mutation.putColumn(Utils.longToBytes(entry.getKey()), entry.getValue().toBytes());
                colCount += 1;
            }
        }
        
        tryBatch(batch, colCount);
        ctx.stop();
    }
    
    private void tryBatch(MutationBatch batch, int colCount) throws Exception {
        Timer.Context ctx = batchTimer.time();
        try {
            batch.execute().getResult();
        } catch (Exception ex) {
            System.err.println(String.format("Busted with %d", colCount));
            throw ex;
        } finally {
            ctx.stop();
        }
    }
    
    // be aware of the memory implications of this method.
    public Map<Long, IBitmap> bulkGet(byte[] key, Collection<Long> colNames) throws Exception {
        List<byte[]> byteCols = new ArrayList<byte[]>(colNames.size());
        for (Long col : colNames)
            byteCols.add(Utils.longToBytes(col));
        
        Timer.Context ctx = bulkGetTimer.time();
        Map<Long, IBitmap> map = new HashMap<Long, IBitmap>();
        try {
            ColumnList<byte[]> cols = keyspace.prepareQuery(columnFamily)
                    .getKey(key).withColumnSlice(byteCols).execute().getResult();
            Iterator<Column<byte[]>> it = cols.iterator();
            Column<byte[]> col;
            while (it.hasNext()) {
                col = it.next();
                map.put(Utils.bytesToLong(col.getName()), MemoryBitmap2.wrap(col.getValue(BytesArraySerializer.get())));
            }
        } finally {
            ctx.stop();
            bulkGetHist.update(colNames.size());
        }
        return map;
    }
    
    public IBitmap get(byte[] key, Long col) throws Exception {
        Timer.Context ctx = getTimer.time();
        try {
            byte[] buf = keyspace.prepareQuery(columnFamily)
                    .getKey(key)
                    .getColumn(Utils.longToBytes(col))
                    .execute().getResult().getByteArrayValue();
            return MemoryBitmap2.wrap(buf);
        } finally {
            ctx.stop();
        }
    }
    
    // iterate over all columns, paging through data in a row.
    public void visitAllColumns(byte[] key, int pageSize, ColumnObserver<Long, IBitmap> observer) throws Exception {
        
        Timer.Context ctx = visitAllTimer.time();
        try {
            RowQuery<byte[], byte[]> query = keyspace
                    .prepareQuery(columnFamily)
                    .getKey(key)
                    .autoPaginate(true)
                    .withColumnRange(new RangeBuilder().setLimit(pageSize).build());
            
            ColumnList<byte[]> columnList;
            while (!(columnList = query.execute().getResult()).isEmpty()) {
                for (Column<byte[]> col : columnList) {
                    long colName = Utils.bytesToLong(col.getName());
                    IBitmap bitmap = MemoryBitmap2.wrap(col.getByteArrayValue());
                    observer.observe(key, colName, bitmap);
                }
            }
        } finally {
            ctx.stop();
        }
    }

    public void del(byte[] key, Long col) throws Exception {
        keyspace.prepareColumnMutation(columnFamily, key, Utils.longToBytes(col)).deleteColumn().execute();
    }
}

