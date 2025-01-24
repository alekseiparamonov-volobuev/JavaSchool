package sbp;

import lombok.Getter;
import sbp.school.kafka.service.ConsumerService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sbp.ConsumerApp.ConsumerGroup.B2;
import static sbp.ConsumerApp.ConsumerGroup.C3;
import static sbp.ConsumerApp.ConsumerGroup.F1;

public class ConsumerApp {

    public static void main(String[] args) {
        ConsumerService f1Service1 = new ConsumerService(F1.getGroup());
        ConsumerService f1Service2 = new ConsumerService(F1.getGroup());
        ConsumerService b2Service1 = new ConsumerService(B2.getGroup());
        ConsumerService c3Service1 = new ConsumerService(C3.getGroup());
        ConsumerService c3Service2 = new ConsumerService(C3.getGroup());
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        executorService.submit(f1Service1);
        executorService.submit(f1Service2);
        executorService.submit(b2Service1);
        executorService.submit(c3Service1);
        executorService.submit(c3Service2);

    }

    enum ConsumerGroup {
        F1("1"),
        B2("2"),
        C3("3");

        @Getter
        private final String group;

        ConsumerGroup(String group) {
            this.group = group;
        }
    }
}
