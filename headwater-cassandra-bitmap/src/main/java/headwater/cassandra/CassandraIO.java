package headwater.cassandra;

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
import headwater.data.ColumnObserver;
import headwater.data.IO;


public class CassandraIO implements IO {

    private final Keyspace keyspace;
    private final ColumnFamily<byte[], byte[]> columnFamily;
    private final AstyanaxContext.Builder builder;

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
        keyspace.prepareColumnMutation(columnFamily, key, col).putValue(value, null).execute();
    }
    
    public byte[] get(byte[] key, byte[] col) throws Exception {
        return keyspace.prepareQuery(columnFamily)
                .getKey(key)
                .getColumn(col)
                .execute().getResult().getByteArrayValue();
    }
    
    // iterate over all columns, paging through data in a row.
    public void visitAllColumns(byte[] key, int pageSize, ColumnObserver observer) throws Exception {
        
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
    }

    public void del(byte[] key, byte[] col) throws Exception {
        keyspace.prepareColumnMutation(columnFamily, key, col).deleteColumn().execute();
    }
}
