package net.hydromatic.optiq.test;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.hydromatic.linq4j.AbstractQueryable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.Pair;
import org.junit.Test;

public class TableInRootSchemaTest {

    @Test
    public void testAddingTableInRootSchema() throws Exception {

        Class.forName("net.hydromatic.optiq.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:optiq:");
        OptiqConnection optiqConnection = connection.unwrap(OptiqConnection.class);

        MutableSchema schema = optiqConnection.getRootSchema();
        schema = MapSchema.create(schema, "BAZ");

        SimpleTable.create(schema, "SAMPLE");

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select A, SUM(B) from BAZ.SAMPLE group by A");

        print(resultSet);
        resultSet.close();
        statement.close();
        connection.close();
    }

    void print(ResultSet resultSet) throws SQLException {

        PrintStream out = System.out;

        ResultSetMetaData meta = resultSet.getMetaData();
        int n = meta.getColumnCount();
        for (int i = 1; i <= n; i++) {
            out.print("\t");
            out.print(meta.getColumnLabel(i));
        }
        out.println();
        out.println("--------------------------");
        while (resultSet.next()) {
            for (int i = 1; i <= n; i++) {
                out.print("\t");
                out.print(resultSet.getObject(i));
            }
            out.println();
        }
    }

    public static class SimpleTable extends AbstractQueryable<Object[]> implements Table<Object[]> {
        private final Schema schema;
        private final String tableName;
        private final RelDataType rowType;
        private String[] columnNames = { "A", "B" };
        private Class[] columnTypes = { String.class, Integer.class };
        private Object[][] rows = new Object[3][];

        SimpleTable(Schema schema, String tableName) {

            this.schema = schema;
            this.tableName = tableName;

            assert schema != null;
            assert tableName != null;

            this.rowType = deduceTypes(schema.getTypeFactory());

            rows[0] = new Object[] { "foo", 5 };
            rows[1] = new Object[] { "bar", 4 };
            rows[2] = new Object[] { "foo", 3 };
        }

        RelDataType deduceTypes(JavaTypeFactory typeFactory) {

            int columnCount = columnNames.length;
            final List<Pair<String, RelDataType>> columnDesc = new ArrayList<Pair<String, RelDataType>>(columnCount);
            for (int i = 0; i < columnCount; i++) {

                final RelDataType colType = typeFactory.createJavaType(columnTypes[i]);
                columnDesc.add(Pair.of(columnNames[i], colType));
            }
            return typeFactory.createStructType(columnDesc);
        }

        public static SimpleTable create(MutableSchema schema, String tableName) {
            SimpleTable table = new SimpleTable(schema, tableName);
            schema.addTable(new TableInSchemaImpl(schema, tableName, Schema.TableType.TABLE, table));
            return table;
        }

        @Override
        public String toString() {
            return "SimpleTable {" + tableName + "}";
        }

        public QueryProvider getProvider() {
            return schema.getQueryProvider();
        }

        public Class getElementType() {
            return Object[].class;
        }

        public RelDataType getRowType() {
            return rowType;
        }

        public Statistic getStatistic() {
            return Statistics.UNKNOWN;
        }

        public Expression getExpression() {
            return Expressions.convert_(
                    Expressions.call(
                            schema.getExpression(),
                            "getTable",
                            Expressions.<Expression> list().append(Expressions.constant(tableName))
                                    .append(Expressions.constant(getElementType()))), SimpleTable.class);
        }

        public Iterator<Object[]> iterator() {
            return Linq4j.enumeratorIterator(enumerator());
        }

        public Enumerator<Object[]> enumerator() {
            return new Enumerator<Object[]>() {

                private Object[] current;
                private Iterator<Object[]> iterator = Arrays.asList(rows).iterator();

                public Object[] current() {
                    return current;
                }

                public boolean moveNext() {
                    if (iterator.hasNext()) {
                        Object[] full = iterator.next();
                        current = full;
                        return true;
                    } else {
                        current = null;
                        return false;
                    }
                }

                public void reset() {
                    throw new UnsupportedOperationException();
                }

                public void close() {
                    // noop
                }

            };
        }
    }
}
