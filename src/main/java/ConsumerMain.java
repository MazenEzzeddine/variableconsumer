import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class ConsumerMain {
    private static final Logger log = LogManager.getLogger(ConsumerMain.class);
   public static KafkaConsumer<String, Customer> consumer = null;

    static double eventsViolating = 0;
    static double eventsNonViolating = 0;
    static double totalEvents = 0;

    static double sleep;


    static Double maxConsumptionRatePerConsumer1 = 0.0d;

    //static NormalDistribution dist = new NormalDistribution(0.25, 0.025);
    // static ExponentialDistribution dist = new ExponentialDistribution(5/*0.2*/);
    //static LogNormalDistribution dist = new LogNormalDistribution(1.6, 0.3);
    static ParetoDistribution dist = new ParetoDistribution(3, 2.75);
    //static ParetoDistribution dist = new ParetoDistribution(3, 2.5);
  //  static ParetoDistribution dist = new ParetoDistribution(2.5, 2); //this
    //static ParetoDistribution dist = new ParetoDistribution(2, 2);
    //static ParetoDistribution dist = new ParetoDistribution(1.2, 1.3);
    // static ParetoDistribution dist = new ParetoDistribution(3, 2);
    //static ParetoDistribution dist = new ParetoDistribution(2, 2.1);
    //static NormalDistribution dist = new NormalDistribution(5, 0.75);



    public ConsumerMain() throws
            IOException, URISyntaxException, InterruptedException {
    }


    public static void main(String[] args)
            throws IOException, URISyntaxException, InterruptedException {






        PrometheusUtils.initPrometheus();
        KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();
        log.info(KafkaConsumerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaConsumerConfig.createProperties(config);
       props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
               StickyAssignor.class.getName());
    /*    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                BinPackPartitionAssignor.class.getName());*/

        // boolean commit = !Boolean.parseBoolean(config.getEnableAutoCommit());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(config.getTopic()), new RebalanceListener());
        KafkaClientMetrics consumerKafkaMetrics = new KafkaClientMetrics(consumer);
        consumerKafkaMetrics.bindTo(PrometheusUtils.prometheusRegistry);

        log.info("Subscribed to topic {}", config.getTopic());

        addShutDownHook();

        sleep = Double.parseDouble(System.getenv("SLEEP"));

        if (sleep == 0) {
            dblogic();
        } else {
            sleeplogic();
        }
        //DatabaseUtils.getAllRows();
    }

    private static void sleeplogic() {

        try {
            while (true) {
                ConsumerRecords<String, Customer> records = consumer.poll
                        (Duration.ofMillis(Long.MAX_VALUE));
                if (records.count() != 0) {
                    for (ConsumerRecord<String, Customer> record : records) { //Customer
                        totalEvents++;
                        //TODO sleep per record or per batch
                        // try {
                        // double sleep=  dist.sample();
                        log.info("sleep is {}", sleep);
                        log.info(" long sleep  {}", (long) sleep);

                        long before = System.currentTimeMillis();

                        try {
                            Thread.sleep((long) sleep);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }

                        long after = System.currentTimeMillis();

                        PrometheusUtils.processingTime
                                .setDuration(after-before);
                        PrometheusUtils.totalLatencyTime
                                .setDuration(System.currentTimeMillis() - record.timestamp());
                        PrometheusUtils.distributionSummary.record(after-before);

                        if (System.currentTimeMillis() - record.timestamp() <= 500 ) {
                            eventsNonViolating++;
                        } else {
                            eventsViolating++;
                        }
                        //log.info("processing time : {}", processingTime);
                        log.info(" latency is {}", System.currentTimeMillis() - record.timestamp());
                       /* } catch (InterruptedException e) {
                            e.printStackTrace();
                        }*/
                    }
                }


                log.info("fraction violating: {}", eventsNonViolating/totalEvents);
                consumer.commitSync();
                log.info("In this poll, received {} events", records.count());
            }
        } catch (WakeupException e) {
            // e.printStackTrace();
        } finally {
            consumer.close();
            log.info("Closed consumer and we are done");
        }
    }

    private static void dblogic() {
        DatabaseUtils.loadAndGetConnection();

        try {
            while (true) {
                ConsumerRecords<String, Customer> records = consumer.poll
                        (Duration.ofMillis(Long.MAX_VALUE));
                if (records.count() != 0) {
                    for (ConsumerRecord<String, Customer> record : records) { //Customer

                        totalEvents++;
                        //TODO sleep per record or per batch
                        // try {
                        // double sleep=  dist.sample();
                        log.info("sleep is {}", sleep);
                        log.info(" long sleep  {}", (long) sleep);

                        //instead of sleep, call fibo top stress cpu
                        // Thread.sleep((long) sleep);

                        log.info("key {}", record.value().getID());
                        log.info("name {}", record.value().getName());

                        long before = System.currentTimeMillis();

                        DatabaseUtils.InsertRow(record.value().getID(), record.value().getName());
                        //fibo(1000);
                        long after = System.currentTimeMillis();

                        PrometheusUtils.processingTime
                                .setDuration(after-before);
                        PrometheusUtils.totalLatencyTime
                                .setDuration(System.currentTimeMillis() - record.timestamp());
                        PrometheusUtils.distributionSummary.record(after-before);

                        if (System.currentTimeMillis() - record.timestamp() <= 500 ) {
                            eventsNonViolating++;
                        } else {
                            eventsViolating++;
                        }
                        //log.info("processing time : {}", processingTime);
                        log.info(" latency is {}", System.currentTimeMillis() - record.timestamp());
                       /* } catch (InterruptedException e) {
                            e.printStackTrace();
                        }*/
                    }
                }


                log.info("fraction violating: {}", eventsNonViolating/totalEvents);
                consumer.commitSync();
                log.info("In this poll, received {} events", records.count());
            }
        } catch (WakeupException e) {
            // e.printStackTrace();
        } finally {
            consumer.close();
            log.info("Closed consumer and we are done");
        }

    }

    static BigDecimal fibo(int n) {
        if(n==0 || n==1) return new BigDecimal(1);
        return new BigDecimal(n).multiply(fibo(n-1));
    }

    private static void addShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Starting exit...");
                consumer.wakeup();
                try {
                    this.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}