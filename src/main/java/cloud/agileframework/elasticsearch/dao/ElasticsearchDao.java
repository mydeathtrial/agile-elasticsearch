package cloud.agileframework.elasticsearch.dao;

import cloud.agileframework.common.util.clazz.ClassUtil;
import cloud.agileframework.common.util.db.SessionUtil;
import cloud.agileframework.common.util.string.StringUtil;
import cloud.agileframework.data.common.dao.BaseDao;
import cloud.agileframework.data.common.dao.ColumnName;
import cloud.agileframework.data.common.dictionary.DataExtendManager;
import com.alibaba.druid.DbType;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentEntity;
import org.springframework.data.elasticsearch.repository.support.MappingElasticsearchEntityInformation;
import org.springframework.data.elasticsearch.repository.support.SimpleElasticsearchRepository;
import org.springframework.data.repository.PagingAndSortingRepository;

import javax.sql.ConnectionPoolDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticsearchDao implements BaseDao {
    private final ElasticsearchRestTemplate restTemplate;

    private final ConnectionPoolDataSource connectionPoolDataSource;

    @Autowired
    private DataExtendManager dataExtendManager;

    public ElasticsearchDao(ElasticsearchRestTemplate restTemplate, ConnectionPoolDataSource connectionPoolDataSource) {
        this.restTemplate = restTemplate;
        this.connectionPoolDataSource = connectionPoolDataSource;
    }

    @Override
    public DataExtendManager dictionaryManager() {
        return dataExtendManager;
    }

    @Override
    public <T, ID> PagingAndSortingRepository<T, ID> getRepository(Class<T> tableClass) {
        PagingAndSortingRepository<T, ID> pagingAndSortingRepository = REPOSITORY_CACHE.get(tableClass);
        if (pagingAndSortingRepository != null) {
            return pagingAndSortingRepository;
        }

        pagingAndSortingRepository = new SimpleElasticsearchRepository<>(new MappingElasticsearchEntityInformation<>((ElasticsearchPersistentEntity<T>) restTemplate.getElasticsearchConverter().getMappingContext().getRequiredPersistentEntity(tableClass)), restTemplate);
        REPOSITORY_CACHE.put(tableClass, pagingAndSortingRepository);
        return pagingAndSortingRepository;
    }

    @SneakyThrows
    @Override
    public Connection getConnection() {
        return connectionPoolDataSource.getPooledConnection().getConnection();
    }

    @Override
    public <T> Page<T> page(T object, PageRequest pageRequest) {
        if (object instanceof Class) {
            throw new IllegalArgumentException("Parameter must be of type POJO");
        }
        if (object == null) {
            return Page.empty();
        }

        List<T> content = findBySQL(toPageSQL(object, pageRequest, DbType.mysql), (Class<T>) object.getClass(), null);
        dictionaryManager().cover(content);
        Long total = findOne(toPageCountSQL(object, pageRequest, DbType.mysql), long.class, null);
        return new PageImpl<>(content, pageRequest, total);
    }

    @Override
    public <T> Page<T> pageBySQL(String sql, int page, int size, Class<T> clazz, Object parameters) {
        List<T> content = Lists.newArrayList();
        long total = 0;
        try (Connection connection = getConnection()) {
            content = SessionUtil.limit(connection, sql, clazz, parameters, size * (page - 1), size);
            dictionaryManager().cover(content);
            total = SessionUtil.count(connection, sql, parameters);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new PageImpl<>(content, PageRequest.of(page - 1, size), total);
    }

    @Override
    public <T> List<T> findBySQL(String sql, Class<T> clazz, Object parameters) {
        try (Connection connection = getConnection()) {
            List<T> content = SessionUtil.query(connection, sql, clazz, parameters);
            dictionaryManager().cover(content);
            return content;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Lists.newArrayList();
    }

    @Override
    public <T> List<T> findBySQL(String sql, Class<T> clazz, Integer firstResult, Integer maxResults, Object parameters) {
        if (maxResults <= firstResult) {
            throw new IllegalArgumentException("maxResults must Greater than or equal to firstResult");
        }
        try (Connection connection = getConnection()) {
            List<T> content = SessionUtil.limit(connection, sql, clazz, parameters, firstResult, maxResults - firstResult);
            dictionaryManager().cover(content);
            return content;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Lists.newArrayList();
    }

    @Override
    public List<Map<String, Object>> findBySQL(String sql, Object parameters) {
        try (Connection connection = getConnection()) {
            return SessionUtil.query(connection, sql, parameters);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Lists.newArrayList();
    }

    @Override
    public int updateBySQL(String sql, Object parameters) {
        try (Connection connection = getConnection()) {
            return SessionUtil.update(connection, sql, parameters);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public <T> List<ColumnName> toColumnNames(Class<T> tableClass) {
        return ClassUtil.getAllEntityAnnotation(tableClass, Field.class).stream().map(f -> {
            ColumnName columnName = new ColumnName();

            Field annotation = f.getAnnotation();
            String value = annotation.value();
            String name = annotation.name();
            if (!StringUtil.isEmpty(value)) {
                columnName.setName(value);
            }
            if (!StringUtil.isEmpty(name)) {
                columnName.setName(name);
            }

            columnName.setMember(f.getMember());

            //设置主键
            Id id = ClassUtil.getFieldAnnotation(tableClass, columnName.getName(), Id.class);
            columnName.setPrimaryKey(id != null);
            return columnName;
        }).collect(Collectors.toList());
    }

    @Override
    public <T> String toTableName(Class<T> tableClass) {
        Document document = tableClass.getAnnotation(Document.class);
        return document.indexName();
    }

    @Override
    public <T> void batchInsert(List<T> list, int batchSize) {
        List<String> sqls = list.stream()
                .map(s -> toInsertSql(s, DbType.mysql))
                .collect(Collectors.toList());

        try (Connection connection = getConnection()) {
            SessionUtil.batchUpdate(connection, sqls);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
