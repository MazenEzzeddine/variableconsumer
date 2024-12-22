import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class PrometheusUtils {
    public static PrometheusMeterRegistry prometheusRegistry;
    public static TimeMeasure processingTime;
    public static Gauge processingGauge;
    public static TimeMeasure totalLatencyTime;
    public static Gauge totalLatencyGauge;

    public static DistributionSummary distributionSummary;


    public static void initPrometheus() {
        prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
            server.createContext("/prometheus", httpExchange -> {
                String response = prometheusRegistry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });
            new Thread(server::start).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        processingTime = new TimeMeasure(0.0);
        processingGauge = Gauge.builder("processingGauge", processingTime, TimeMeasure::getDuration).register(prometheusRegistry);

        totalLatencyTime = new TimeMeasure(0.0);
        totalLatencyGauge = Gauge.builder("totallatencygauge", totalLatencyTime, TimeMeasure::getDuration).register(prometheusRegistry);


        distributionSummary = DistributionSummary.builder("events_latency").register(prometheusRegistry);

    }
}