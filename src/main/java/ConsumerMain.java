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
  // public static KafkaConsumer<String, String> consumer = null;

    static double eventsViolating = 0;
    static double eventsNonViolating = 0;
    static double totalEvents = 0;


    static Double maxConsumptionRatePerConsumer1 = 0.0d;
    //Long[] waitingTimes = new Long[10];

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

        //Thread.sleep(4000);
        PrometheusUtils.initPrometheus();
        KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();
        log.info(KafkaConsumerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaConsumerConfig.createProperties(config);
/*        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                StickyAssignor.class.getName());*/

        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                BinPackPartitionAssignor.class.getName());

        // boolean commit = !Boolean.parseBoolean(config.getEnableAutoCommit());
        //consumer = new KafkaConsumer<String, Customer>(props);
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(config.getTopic()), new RebalanceListener());

        KafkaClientMetrics consumerKafkaMetrics = new KafkaClientMetrics(consumer);
        consumerKafkaMetrics.bindTo(PrometheusUtils.prometheusRegistry);


        log.info("Subscribed to topic {}", config.getTopic());

        addShutDownHook();
        double sumProcessing = 0;
        try {
            while (true) {
      /*          ConsumerRecords<String, Customer> records = consumer.poll
                        (Duration.ofMillis(Long.MAX_VALUE));*/
                ConsumerRecords<String, Customer> records = consumer.poll
                        (Duration.ofMillis(Long.MAX_VALUE));
                if (records.count() != 0) {
                    for (ConsumerRecord<String, Customer> record : records) { //Customer
                        //
                        totalEvents++;
                        //TODO sleep per record or per batch
                        try {
                            double sleep = 5;
                            // double sleep=  dist.sample();

                            log.info("sleep is {}", sleep);
                            log.info(" long sleep  {}", (long) sleep);

                            //instead of sleep, call fibo top stress cpu
                            Thread.sleep((long) sleep);
                            //fibo(1000);
                          //  sumProcessing += sleep;
                                PrometheusUtils.processingTime
                                        .setDuration(sleep);
                            PrometheusUtils.totalLatencyTime
                                    .setDuration(System.currentTimeMillis() - record.timestamp());
                            PrometheusUtils.distributionSummary.record(sleep);
                            PrometheusUtils.timer.record((long)sleep, TimeUnit.MILLISECONDS);

                            if (System.currentTimeMillis() - record.timestamp() <= 500 /*1500*/) {
                                eventsNonViolating++;
                            } else {
                                eventsViolating++;
                            }
                            //log.info("processing time : {}", processingTime);
                            log.info(" latency is {}", System.currentTimeMillis() - record.timestamp());
                            // record.timestamp(), System.currentTimeMillis();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

        /* PrometheusUtils.processingTime
                        .setDuration(max);*/
                PrometheusUtils.processingTime
                        .setDuration(sumProcessing / records.count());




                log.info("Average processing latency is {}", sumProcessing / records.count() );
                sumProcessing = 0;

                consumer.commitSync();
                log.info("In this poll, received {} events", records.count());

                  /*  Long timeAfterPollingProcessingAndCommit = System.currentTimeMillis();
                    ConsumptionRatePerConsumerInThisPoll = ((float) records.count() /
                            (float) (timeAfterPollingProcessingAndCommit - timeBeforePolling)) * 1000.0f;
                    pollsSoFar += 1;
                    averageRatePerConsumerForGrpc = averageRatePerConsumerForGrpc +
                            (ConsumptionRatePerConsumerInThisPoll -
                                    averageRatePerConsumerForGrpc) / (float) (pollsSoFar);

                    if (maxConsumptionRatePerConsumer < ConsumptionRatePerConsumerInThisPoll) {
                        maxConsumptionRatePerConsumer = ConsumptionRatePerConsumerInThisPoll;
                    }
                    log.info("ConsumptionRatePerConsumerInThisPoll in this poll {}",
                            ConsumptionRatePerConsumerInThisPoll);
                    log.info("maxConsumptionRatePerConsumer {}", maxConsumptionRatePerConsumer);
                    double percentViolating = eventsViolating / totalEvents;
                    double percentNonViolating = eventsNonViolating / totalEvents;
                    log.info("Percent violating so far {}", percentViolating);
                    log.info("Percent non violating so far {}", percentNonViolating);
                    log.info("total events {}", totalEvents);*/
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