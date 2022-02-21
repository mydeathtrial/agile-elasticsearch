package com.agile;

import cloud.agileframework.elasticsearch.config.ElasticsearchDaoAutoConfiguration;
import cloud.agileframework.elasticsearch.dao.ElasticsearchDao;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;

import javax.annotation.Resource;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootTest(classes = {App.class, ElasticsearchDaoAutoConfiguration.class})
public class TestSpringDataEs {
    @Resource
    private ElasticsearchDao esDao;

    @Test
    public void toSQL() {
        String sql = esDao.toUpdateSql(Persons.builder().id("123")
                .name("tongmeng")
                .sex(true)
                .age(18)
                .build(), DbType.mysql);
        System.out.println(sql);
    }

    @Test
    public void updateSQL() {
        String sql = "INSERT INTO persons7(name, sex, id, age,_id) VALUES ('tongmeng', true, '123', 18,'id1'),('tongmeng2', true, '123', 18,'id1')";
        esDao.updateBySQL(sql,null);
    }

    @Test
    public void selectGroupSQL() {
        String sql = "select count(name) from persons7 group by name,age";
        esDao.findBySQL(sql,null);
    }

    @Test
    public void save() {
        esDao.deleteAll(Persons.class);
        String id = UUID.randomUUID().toString();
        String name = "xxx";
        esDao.save(Persons.builder().id(id)
                .name(name)
                .sex(true)
                .age(11)
                .build());

        Assertions.assertEquals(esDao.findOne(Persons.class, id).getName(), name);
    }

    @Test
    public void update() {
        esDao.deleteAll(Persons.class);
        String id = UUID.randomUUID().toString();
        String name = "xxx";
        esDao.save(Persons.builder().id(id)
                .name("tongmeng")
                .sex(true)
                .age(11)
                .build());

        esDao.update(Persons.builder().id(id)
                .name(name)
                .sex(false)
                .age(12)
                .build());
        Persons one = esDao.findOne(Persons.class, id);
        Assertions.assertEquals(one.getName(), name);
        Assertions.assertEquals(one.getAge(), 12);
        Assertions.assertEquals(one.getSex(), false);
        
    }
    
    @Test
    public void findOne(){
        esDao.deleteAll(Persons.class);
        String id = UUID.randomUUID().toString();
        String name = "xxx";
        esDao.save(Persons.builder().id(id)
                .name(name)
                .sex(true)
                .age(11)
                .build());

        Assertions.assertEquals(esDao.findOne("select name from persons7 where id = ${id}",String.class,Persons.builder().id(id).build()), name);
    }

    @Test
    public void saveAll() throws InterruptedException {
        esDao.deleteAll(Persons.class);
        int total = 200;
        //插入数据
        List<Persons> list = IntStream.range(0, total)
                .mapToObj(i -> Persons.builder().id(UUID.randomUUID().toString())
                        .name("tongmeng")
                        .sex(true)
                        .age(i)
                        .build())
                .collect(Collectors.toList());
        esDao.save(list);
        Thread.sleep(2000);
        Assertions.assertEquals((long) esDao.findAllByClass(Persons.class).size(), total);
    }

    @Test
    public void saveBatch() throws InterruptedException {
        esDao.deleteAll(Persons.class);
        int total = 100;
        //插入数据
        List<Persons> list = IntStream.range(0, total)
                .mapToObj(i -> Persons.builder().id(UUID.randomUUID().toString())
                        .name("tongmeng")
                        .sex(true)
                        .age(i)
                        .build())
                .collect(Collectors.toList());

        List<Persons> list2 = IntStream.range(0, total)
                .mapToObj(i -> Persons.builder().id(UUID.randomUUID().toString())
                        .name("tongmeng")
                        .sex(false)
                        .age(i)
                        .build())
                .collect(Collectors.toList());
        list.addAll(list2);
        esDao.batchInsert(list);

        Thread.sleep(2000);
        Assertions.assertEquals((long) esDao.findAllByClass(Persons.class).size(), total*2);
    }

    /**
     * sql分页
     */
    @Test
    public void test() throws InterruptedException {
        saveBatch();
        Page<Persons> page = esDao.page(Persons.builder().sex(true).build(), 1, 10);
        Assertions.assertNotNull(page);
        Assertions.assertTrue(page.getTotalElements() > 0);
        Assertions.assertTrue(page.getTotalPages() > 0);
        Assertions.assertEquals(10, page.getContent().size());
    }

    /**
     * 自动化条件查询
     */
    @Test
    public void findAll() throws InterruptedException {
        saveBatch();
        esDao.save(Persons.builder().name("xxx").sex(true).age(16).build());
        List<Persons> a = esDao.findAll(Persons.builder().name("xxx").sex(true).age(16).build());
        Assertions.assertFalse(a.isEmpty());
        Assertions.assertEquals(a.get(0).getName(),"xxx");
    }

    /**
     * 自动化条件查询
     */
    @Test
    public void findAllByClass() throws InterruptedException {
        saveBatch();
        List<Persons> l = esDao.findAllByClass(Persons.class);
        Assertions.assertEquals(l.size(),200);
    }

    /**
     * 自动化条件查询
     */
    @Test
    public void pageBySQL() throws InterruptedException {
        saveBatch();
        Page<Persons> page = esDao.pageBySQL("select * from persons7 where sex = #{sex}", 1, 100, Persons.class, Persons.builder().sex(true).build());
        Assertions.assertEquals(page.getTotalElements(),100);
        Assertions.assertEquals(page.getContent().size(),100);
        Assertions.assertEquals(page.getTotalPages(), 1);
    }
}
