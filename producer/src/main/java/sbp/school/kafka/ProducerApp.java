package sbp.school.kafka;

import sbp.school.kafka.service.ProducerService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProducerApp {

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        ProducerService producerService = new ProducerService();
        for (int i = 0; i < 20; i++) {
            executorService.submit(producerService);
        }
        executorService.shutdown();
    }
}
