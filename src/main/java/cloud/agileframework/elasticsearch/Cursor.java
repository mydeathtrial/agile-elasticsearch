package cloud.agileframework.elasticsearch;


import cloud.agileframework.elasticsearch.types.ElasticsearchType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Cursor {
    private Schema schema;
    private List<Row> rows;
    private int currentRow = -1;
    private Map<String, Integer> labelToIndexMap;

    public Cursor(Schema schema, List<Row> rows) {
        this.schema = schema;
        this.rows = rows;
        initLabelToIndexMap();
    }

    public Schema getSchema() {
        return schema;
    }

    public Object getColumn(int index) {
        if (index < 0 || index >= getColumnCount())
            throw new IllegalArgumentException("Column Index out of range: " + index);
        return rows.get(currentRow).get(index);
    }

    public int getColumnCount() {
        return schema.getNumberOfColumns();
    }

    public boolean next() {
        if (currentRow < rows.size() - 1) {
            currentRow++;
            return true;
        } else {
            return false;
        }
    }

    public Integer findColumn(String label) {
        return labelToIndexMap.get(label);
    }

    private void initLabelToIndexMap() {
        labelToIndexMap = new HashMap<>();
        for (int i=0; i < schema.getNumberOfColumns(); i++) {
            ColumnMetaData columnMetaData = schema.getColumnMetaData(i);
            labelToIndexMap.put(columnMetaData.getLabel(), i);
        }
    }

    public static class Row {
        private List<Object> columnData;

        public Row(List<Object> columnData) {
            this.columnData = columnData;
        }

        public Object get(int index) {
            return columnData.get(index);
        }
    }

    public static class Schema {
        private final List<ColumnMetaData> columnMetaDataList;
        private final int numberOfColumns;

        public Schema(List<ColumnMetaData> columnMetaDataList) {
            this.columnMetaDataList = columnMetaDataList;
            this.numberOfColumns = columnMetaDataList != null ? columnMetaDataList.size() : 0;
        }

        /**
         * @return Number of columns in result
         */
        public int getNumberOfColumns() {
            return this.numberOfColumns;
        }

        /**
         * Returns {@link ColumnMetaData} for a specific column in the result
         *
         * @param index the index of the column to return metadata for
         *
         * @return {@link ColumnMetaData} for the specified column
         */
        public ColumnMetaData getColumnMetaData(int index) {
            return columnMetaDataList.get(index);
        }

        /**
         * Returns the {@link ElasticsearchType} corresponding to a specific
         * column in the result.
         *
         * @param index the index of the column to return the type for
         *
         * @return {@link ElasticsearchType} for the specified column
         */
        public ElasticsearchType getElasticsearchType(int index) {
            return columnMetaDataList.get(index).getEsType();
        }
    }

}
