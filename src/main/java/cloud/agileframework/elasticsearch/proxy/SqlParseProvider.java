package cloud.agileframework.elasticsearch.proxy;

import cloud.agileframework.common.util.clazz.ClassUtil;
import cloud.agileframework.common.util.clazz.TypeReference;
import cloud.agileframework.common.util.object.ObjectUtil;
import cloud.agileframework.common.util.stream.StreamUtil;
import com.alibaba.druid.sql.ast.SQLStatement;

import java.io.InputStream;
import java.lang.reflect.Type;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

public interface SqlParseProvider<P extends JdbcResponse, S extends SQLStatement> {

    /**
     * sql语句转换为请求体
     *
     * @param statement sql语法快
     * @return 请求体
     */
    default JdbcRequest of(S statement) throws SQLFeatureNotSupportedException{
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * sql语句转换为请求体
     *
     * @param statement sql语法快
     * @return 请求体
     */
    default JdbcRequest of(List<S> statement) throws SQLFeatureNotSupportedException{
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * 判断当前解析器是否可以处理该语句
     *
     * @param statement 语法块
     * @return 是否
     */
    default boolean accept(SQLStatement statement) {
        Type clazz = ClassUtil.getGeneric(getClass(), SqlParseProvider.class, 1);
        return ((Class<?>) clazz).isAssignableFrom(statement.getClass());
    }

    default P toResponse(InputStream contentStream) {
        Type clazz = ClassUtil.getGeneric(getClass(), SqlParseProvider.class, 0);
        TypeReference<P> toClass = new TypeReference<>(clazz);
        return ObjectUtil.to(StreamUtil.toString(contentStream), toClass);
    }
}
