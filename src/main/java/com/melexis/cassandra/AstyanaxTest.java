package com.melexis.cassandra;

import com.netflix.astyanax.*;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.*;
import com.netflix.astyanax.impl.*;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.*;

import org.apache.commons.lang.StringUtils;
import org.joda.time.*;

public class AstyanaxTest {

    private static final String ONE_MEGABYTE = StringUtils.repeat("a", 1024 * 1024);

    public static void main(final String[] args) throws Exception {
        final AstyanaxContext<Keyspace> ctx = new AstyanaxContext.Builder()
            .forCluster("local")
            .forKeyspace("TESTNS")
            .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
                                       .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                                       //                                       .setCqlVersion(cqlVersion)
                                       //.setTargetCassandraVersion(cassandraVersion)
                                       .setDefaultReadConsistencyLevel(ConsistencyLevel.CL_LOCAL_QUORUM)
                                       .setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_LOCAL_QUORUM)
                                       )
            .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                                             .setPort(9160)
                                             .setMaxConnsPerHost(1)
                                             .setSeeds("esb-a-test:9160, esb-b-test:9160")
                                             .setLocalDatacenter("ieper")
                                             )
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());

        ctx.start();
        final Keyspace ks = ctx.getClient();

        final ColumnFamily<String, String> CF_TEST = 
            new ColumnFamily<String, String>("TESTKS",
                                             StringSerializer.get(),
                                             StringSerializer.get());


        int written = 0;
        int failures = 0;
        final DateTime before = new DateTime();

        for (int r=0; r<50; r++) {
            System.out.println("Start write of row " + r);
            for (int c=0; c<50; c++) {
                written++;
                ks.prepareColumnMutation(CF_TEST, "row" + r, "column" + c)
                    .putValue(ONE_MEGABYTE, null)
                    .execute();
            }
            System.out.println("Stop write of row " + r);
        }

        final Period delta = new Period(before, new DateTime());

        System.out.println(String.format("Wrote %d MB in %s.  Got %d failures", 
                                         written,
                                         delta,
                                         failures));                                                                         
    }
}
