package cloud.agileframework.elasticsearch;


import cloud.agileframework.elasticsearch.types.ElasticsearchType;

import java.sql.SQLFeatureNotSupportedException;

public class ColumnMetaData {
    private String name;
    private String label;
    private String tableSchemaName;
    private int precision = -1;
    private int scale = -1;
    private String tableName;
    private String catalogName;
    private String esTypeName;
    private ElasticsearchType esType;

    public ColumnMetaData(AgileResultSet.Column descriptor) throws SQLFeatureNotSupportedException {
        this.name = descriptor.getName();

        // if a label isn't specified, the name is the label
        this.label = descriptor.getLabel() == null ? this.name : descriptor.getLabel();

        this.esTypeName = descriptor.getType();
        this.esType = ElasticsearchType.fromTypeName(esTypeName);

        // use canned values until server can return this
        this.precision = this.esType.getPrecision();
        this.scale = 0;

        // JDBC has these, but our protocol does not yet convey these
        this.tableName = "";
        this.catalogName = "";
        this.tableSchemaName = "";
    }

    public String getName() {
        return name;
    }

    public String getLabel() {
        return label;
    }

    public String getTableSchemaName() {
        return tableSchemaName;
    }

    public int getPrecision() {
        return  precision;
    }

    public int getScale() {
        return scale;
    }

    public String getTableName() {
        return tableName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public ElasticsearchType getEsType() {
        return esType;
    }

    public String getEsTypeName() {
        return esTypeName;
    }
}
