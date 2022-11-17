package com.jrg.spring.integration.kafka.publisher;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jrg.spring.integration.kafka.publisher.component.BookPublisher;
import com.jrg.spring.integration.kafka.publisher.model.Book;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.GenericMessage;

@SpringBootApplication
public class LibraryApplication {

    @Autowired
    private BookPublisher bookPublisher;

    public static void main(String[] args) {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(LibraryApplication.class).run(args);
        context.getBean(LibraryApplication.class).run(context);
        context.close();
    }

    private void run(ConfigurableApplicationContext context) {

        System.out.println("Inside ProducerApplication run method...");

        MessageChannel producerChannel = context.getBean("producerChannel", MessageChannel.class);

        List<Book> books = bookPublisher.getBooks();

        for (Book book : books) {
            System.out.println("Publishing book " + book.toString());
            Map headers = Collections.singletonMap(KafkaHeaders.TOPIC, book.getGenre().toString());
            producerChannel.send(new GenericMessage(book.toString(), headers));
        }

        System.out.println("Finished ProducerApplication run method...");
    }

}
