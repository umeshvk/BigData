package com.mvdb.platform.scratch.action;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySerde implements SerDe
{
    private static Logger logger = LoggerFactory.getLogger(MySerde.class);

    int                   numColumns;
    StructObjectInspector rowOI;
    ArrayList<Object>     row;

    BytesWritable         serializeBytesWritable;
    // NonSyncDataOutputBuffer barrStr;
    // TypedBytesWritableOutput tbOut;

    // NonSyncDataInputBuffer inBarrStr;
    // TypedBytesWritableInput tbIn;

    List<String>          columnNames;
    List<TypeInfo>        columnTypes;

    /**
     * @param args
     */
    public static void main(String[] args)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException
    {
        System.out.println("deserialize");
        BytesWritable data = (BytesWritable) blob;
        // inBarrStr.reset(data.getBytes(), 0, data.getLength());

        for (int i = 0; i < columnNames.size(); i++)
        {
            // row.set(i, deserializeField(tbIn, columnTypes.get(i),
            // row.get(i)));
            row.set(i, new IntWritable(1));
        }

        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException
    {
        return rowOI;
    }

    @Override
    public SerDeStats getSerDeStats()
    {
        return null;
    }

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException
    {
        System.out.println("initialize");
        serializeBytesWritable = new BytesWritable();
        // barrStr = new NonSyncDataOutputBuffer();
        // tbOut = new TypedBytesWritableOutput(barrStr);
        //
        // inBarrStr = new NonSyncDataInputBuffer();
        // tbIn = new TypedBytesWritableInput(inBarrStr);

        // Read the configuration parameters
        String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

        columnNames = Arrays.asList(columnNameProperty.split(","));
        columnTypes = null;
        if (columnTypeProperty.length() == 0)
        {
            columnTypes = new ArrayList<TypeInfo>();
        } else
        {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }

        assert columnNames.size() == columnTypes.size();
        numColumns = columnNames.size();

        // All columns have to be primitive.
        for (int c = 0; c < numColumns; c++)
        {
            if (columnTypes.get(c).getCategory() != Category.PRIMITIVE)
            {
                throw new SerDeException(getClass().getName() + " only accepts primitive columns, but column[" + c
                        + "] named " + columnNames.get(c) + " has category " + columnTypes.get(c).getCategory());
            }
        }

        // Constructing the row ObjectInspector:
        // The row consists of some string columns, each column will be a java
        // String object.
        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
        for (int c = 0; c < numColumns; c++)
        {
            columnOIs.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(columnTypes.get(c)));
        }

        // StandardStruct uses ArrayList to store the row.
        rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

        // Constructing the row object, etc, which will be reused for all rows.
        row = new ArrayList<Object>(numColumns);
        for (int c = 0; c < numColumns; c++)
        {
            row.add(null);
        }
    }

    @Override
    public Class<? extends Writable> getSerializedClass()
    {
        return BytesWritable.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException
    {
        // try {
        // barrStr.reset();
        // StructObjectInspector soi = (StructObjectInspector) objInspector;
        // List<? extends StructField> fields = soi.getAllStructFieldRefs();
        //
        // for (int i = 0; i < numColumns; i++) {
        // Object o = soi.getStructFieldData(obj, fields.get(i));
        // ObjectInspector oi = fields.get(i).getFieldObjectInspector();
        // serializeField(o, oi, row.get(i));
        // }
        //
        // // End of the record is part of the data
        // tbOut.writeEndOfRecord();
        //
        // serializeBytesWritable.set(barrStr.getData(), 0,
        // barrStr.getLength());
        // } catch (IOException e) {
        // throw new SerDeException(e.getMessage());
        // }
        // return serializeBytesWritable;
        serializeBytesWritable.set(new byte[] {}, 0, 0);
        return serializeBytesWritable;

    }

}
