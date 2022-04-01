package kr.jclab.grpcoverwebsocket.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class ExampleApplication {

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext application = SpringApplication.run(ExampleApplication.class, args);
        Thread.sleep(1000);
        new ClientApplication().run();
//        application.close();
    }

}
