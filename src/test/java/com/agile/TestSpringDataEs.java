package com.agile;

import cloud.agileframework.common.DataException;
import cloud.agileframework.dictionary.util.TranslateException;
import cloud.agileframework.elasticsearch.config.ElasticsearchDaoAutoConfiguration;
import cloud.agileframework.elasticsearch.dao.ElasticsearchDao;
import com.alibaba.druid.DbType;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;

import javax.annotation.Resource;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
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
        esDao.updateBySQL(sql);
    }

    @Test
    public void selectGroupSQL() {
        String sql = "select * from persons7";
        esDao.findBySQL(sql, Maps.newHashMap());

        String sql2 = "select a.* from persons7 as a left join persons7 as b on a.id = b.id";
        esDao.findBySQL(sql2, Maps.newHashMap());
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
    public void findOne() {
        esDao.deleteAll(Persons.class);
        String id = UUID.randomUUID().toString();
        String name = "xxx";
        esDao.save(Persons.builder().id(id)
                .name(name)
                .sex(true)
                .age(11)
                .build());

        Assertions.assertEquals(esDao.findOne("select name from persons7 where id = #{id}", String.class, Persons.builder().id(id).build()), name);
    }

    @Test
    public void saveAll() throws InterruptedException, TranslateException {
        esDao.deleteAll(Persons.class);
        int total = 200;
        //????????????
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
    public void saveBatch() throws InterruptedException, TranslateException {
        esDao.deleteAll(Persons.class);
        int total = 100;
        //????????????
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
        Assertions.assertEquals((long) esDao.findAllByClass(Persons.class).size(), total * 2);
    }

    /**
     * sql??????
     */
    @Test
    public void test() throws InterruptedException, TranslateException {
        saveBatch();
        Page<Persons> page = esDao.page(Persons.builder().sex(true).build(), 2, 10);
        Assertions.assertNotNull(page);
        Assertions.assertTrue(page.getTotalElements() > 0);
        Assertions.assertTrue(page.getTotalPages() > 0);
        Assertions.assertEquals(10, page.getContent().size());
    }

    /**
     * ?????????????????????
     */
    @Test
    public void findAll() throws InterruptedException, DataException {
        saveBatch();
        esDao.save(Persons.builder().name("xxx").sex(true).age(16).build());
        List<Persons> a = esDao.findAll(Persons.builder().name("xxx").sex(true).age(16).build());
        Assertions.assertFalse(a.isEmpty());
        Assertions.assertEquals(a.get(0).getName(), "xxx");
    }

    /**
     * ?????????????????????
     */
    @Test
    public void findAllByClass() throws InterruptedException, TranslateException {
        saveBatch();
        List<Persons> l = esDao.findAllByClass(Persons.class);
        Assertions.assertEquals(l.size(), 200);
    }

    /**
     * ?????????????????????
     */
    @Test
    public void pageBySQL() {
        AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < 1000; i++) {
//           new Thread(()->{
            Page<Persons> data = esDao.pageBySQL("select * from persons7 where sex = #{sex}", 1, 100, Persons.class, Persons.builder().sex(true).build());
            System.out.println("-------cha--------" + data.getTotalElements());
//           }).start();
            count.getAndIncrement();
        }
//       Thread.sleep(40000);
    }
}
