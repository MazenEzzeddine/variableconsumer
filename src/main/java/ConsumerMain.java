import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;


public class ConsumerMain {
    private static final Logger log = LogManager.getLogger(ConsumerMain.class);
    public static KafkaConsumer<String, Customer> consumer = null;
    static double eventsViolating = 0;
    static double eventsNonViolating = 0;
    static double totalEvents = 0;
    static float maxConsumptionRatePerConsumer = 0.0f;
    static float ConsumptionRatePerConsumerInThisPoll = 0.0f;
    static float averageRatePerConsumerForGrpc = 0.0f;
    static long pollsSoFar = 0;

    static ArrayList<TopicPartition> tps;
    static KafkaProducer<String, Customer> producer;


    static Double maxConsumptionRatePerConsumer1 = 0.0d;
    //Long[] waitingTimes = new Long[10];

    //static NormalDistribution dist = new NormalDistribution(0.25, 0.025);


   // static ExponentialDistribution dist = new ExponentialDistribution(5/*0.2*/);


    //static LogNormalDistribution dist = new LogNormalDistribution(1.6, 0.3);

    static ParetoDistribution dist = new ParetoDistribution(3, 2.75);


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

       // boolean commit = !Boolean.parseBoolean(config.getEnableAutoCommit());
        consumer = new KafkaConsumer<String, Customer>(props);
        consumer.subscribe(Collections.singletonList(config.getTopic())/*, new RebalanceListener()*/);
        log.info("Subscribed to topic {}", config.getTopic());

        addShutDownHook();


        double max=0;



        try {
            while (true) {
                ConsumerRecords<String, Customer> records = consumer.poll
                        (Duration.ofMillis(Long.MAX_VALUE));

                if (records.count() != 0) {

                        for (ConsumerRecord<String, Customer> record : records) {

                            //
                            totalEvents++;
                            //TODO sleep per record or per batch
                            try {

                                double sleep = dist.sample();
                                if (max< sleep) {
                                    max=sleep;
                                }

                                log.info("sleep is {}", sleep);
                                log.info(" long sleep  {}", (long)sleep);

                                Thread.sleep((long)sleep);
                               // Thread.sleep(5);


                                PrometheusUtils.processingTime
                                        .setDuration(sleep);
                                PrometheusUtils.totalLatencyTime
                                        .setDuration(System.currentTimeMillis() - record.timestamp());

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

                PrometheusUtils.processingTime
                        .setDuration(max);
                max=0;
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
            }
         catch (WakeupException e) {
            // e.printStackTrace();
        } finally {
            consumer.close();
            log.info("Closed consumer and we are done");
        }
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