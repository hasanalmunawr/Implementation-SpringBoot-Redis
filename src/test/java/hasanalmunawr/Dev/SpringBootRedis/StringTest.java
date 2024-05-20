package hasanalmunawr.Dev.SpringBootRedis;

import io.lettuce.core.api.sync.RedisGeoCommands;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.*;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.domain.geo.GeoLocation;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.not;
import static org.assertj.core.api.FactoryBasedNavigableListAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest
public class StringTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void redisTemplate() {
        Assertions.assertNotNull(redisTemplate);
    }

    @Test
    void valueOperations() throws InterruptedException {
        ValueOperations<String, String> operations = redisTemplate.opsForValue();

        operations.set("name", "hasan", Duration.ofSeconds(3L));
        assertEquals("hasan", operations.get("name"));

        Thread.sleep(5000L);
        assertNull(operations.get("name"));
    }

    @Test
    void listOperation() {
        ListOperations<String, String> operations = redisTemplate.opsForList();

        operations.rightPush("name", "hasan");
        operations.rightPush("name", "deva");
        operations.rightPush("name", "dodi");

        assertEquals("hasan", operations.leftPop("name"));
        assertEquals("deva", operations.leftPop("name"));
        assertEquals("dodi", operations.leftPop("name"));
    }

    @Test
    void setOperation() {
        SetOperations<String, String> operations = redisTemplate.opsForSet();

        operations.add("student", "hasan");
        operations.add("student", "hasan");
        operations.add("student", "almunawar");
        operations.add("student", "almunawar");

        assertEquals(2, operations.members("student").size());
        System.out.printf(operations.members("students").toString());

        redisTemplate.delete("student");
    }

    @Test
    void zSetOperation() {
        ZSetOperations<String, String> operations = redisTemplate.opsForZSet();

        operations.add("score", "hasan", 89);
        operations.add("score", "almu", 99);
        operations.add("score", "budi", 98);

        assertEquals("almu", operations.popMax("score").getValue());
//        assertEquals(99, operations.popMax("score").getScore());
        assertEquals("budi", operations.popMax("score").getValue());
//        assertEquals(98, operations.popMax("score").getScore());
        assertEquals("hasan", operations.popMax("score").getValue());
//        assertEquals(89, operations.popMax("score").getScore());
    }

    @Test
    void hashOperation() {
        HashOperations<String, Object, Object> operations = redisTemplate.opsForHash();

        operations.put("user1", "id", "01");
        operations.put("user1", "name", "hasan");
        operations.put("user1", "prodi", "Information System");

        assertEquals("01", operations.get("user1", "id"));
        assertEquals("hasan", operations.get("user1", "name"));
        assertEquals("Information System", operations.get("user1", "prodi"));

        redisTemplate.delete("user1");
    }

    @Test
    void geoOperation() {
        GeoOperations<String, String> operations = redisTemplate.opsForGeo();

        operations.add("sellers", new Point(106.822702, -6.177590), "Toko A");
        operations.add("sellers", new Point(106.820889, -6.174964), "Toko B");

        Distance distance = operations.distance("sellers", "Toko A", "Toko B", Metrics.KILOMETERS);
        assertEquals(0.3543, distance.getValue());

//        GeoResults<RedisGeoCommands.GeoLocation<String>> sellers =
//                operations.search("sellers", new Circle(
//                        new Point(106.821922, -6.175491),
//                        new Distance(5, Metrics.KILOMETERS)
//                ));

//        assertEquals(2, sellers.getContent().size());
//        assertEquals("Toko A", sellers.getContent().get(0).getContent().getName());
//        assertEquals("Toko B", sellers.getContent().get(1).getContent().getName());
    }


    @Test
    void hyperLogOperation() {
        HyperLogLogOperations<String, String> operations = redisTemplate.opsForHyperLogLog();

        operations.add("traffics", "Hasan", "Almunawar", "KY");
        operations.add("traffics", "eko", "budi", "joko");
        operations.add("traffics", "budi", "joko", "rully");

        assertEquals(7L, operations.size("traffics"));
    }

    @Test
    void trancantion() {
        redisTemplate.execute(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.multi();

                operations.opsForValue().set("test1", "Hasan", Duration.ofSeconds(2));
                operations.opsForValue().set("test2", "Budi", Duration.ofSeconds(2));

                operations.exec();
                return null;
            }
        });

        assertEquals("Hasan", redisTemplate.opsForValue().get("test1"));
        assertEquals("Budi", redisTemplate.opsForValue().get("test2"));
    }

    @Test
    void pipeline() {
        List<Object> statuses = redisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.opsForValue().set("test1", "Eko", Duration.ofSeconds(2));
                operations.opsForValue().set("test2", "Eko", Duration.ofSeconds(2));
                operations.opsForValue().set("test3", "Eko", Duration.ofSeconds(2));
                operations.opsForValue().set("test4", "Eko", Duration.ofSeconds(2));
                return null;
            }
        });

//        assertThat(statuses, hasSize(4));
//        assertThat(statuses, hasItem(true));
//        assertThat(statuses, not(hasItem(false)));
    }

//    STREAM OPERATION
    @Test
    void publishStream() {
        StreamOperations<String, Object, Object> operations = redisTemplate.opsForStream();
        var record = MapRecord.create("stream-1", Map.of(
                "name", "Hasan Almunawar",
                "address", "Jambi"
        ));

        for (int i = 0; i < 10; i++) {
            operations.add(record);

        }
    }

    @Test
    void subcribeStream() {
        StreamOperations<String, Object, Object> operations = redisTemplate.opsForStream();

        try {
            operations.createGroup("stream-1", "sample-group");
        } catch (RedisSystemException exception) {
            // group sudah ada
        }

        List<MapRecord<String, Object, Object>> records = operations.read(Consumer.from("sample-group", "sample-1"),
                StreamOffset.create("stream-1", ReadOffset.lastConsumed()));

        for (MapRecord<String, Object, Object> record : records) {
            System.out.println(record);
        }
    }
}

