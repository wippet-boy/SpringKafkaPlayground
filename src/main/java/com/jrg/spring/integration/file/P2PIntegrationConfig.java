package com.jrg.spring.integration.file;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.integration.annotation.BridgeFrom;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import java.io.File;
import java.util.Scanner;

/**
 * This code should create the DIRS's if not there.  Note in Mac Community intellij version
 * the gui doesn't always refresh the files so it may look like it's not running
 * Check via terminal
 */
@Configuration
@EnableIntegration
public class P2PIntegrationConfig {

    public String INPUT_DIR = "testDIRS/fromDIR";
    public String OUTPUT_DIR1 = "testDIRS/toDIR1";
    public String OUTPUT_DIR2 = "testDIRS/toDIR2";
    public String OUTPUT_DIR3 = "testDIRS/toDIR3";
    public String FILE_PATTERN = "*.txt";

    @Bean
    public MessageChannel pubSubFileChannel() {
        return new PublishSubscribeChannel();
    }

    @Bean
    @BridgeFrom(value = "pubSubFileChannel")
    public MessageChannel fileChannel1() {
        return new DirectChannel();
    }

    @Bean
    @BridgeFrom(value = "pubSubFileChannel")
    public MessageChannel fileChannel2() {
        return new DirectChannel();
    }

    @Bean
    @BridgeFrom(value = "pubSubFileChannel")
    public MessageChannel fileChannel3() {
        return new DirectChannel();
    }

    @Bean
    @InboundChannelAdapter(value = "pubSubFileChannel", poller = @Poller(fixedDelay = "1000"))
    public MessageSource<File> fileReadingMessageSource() {
        FileReadingMessageSource sourceReader = new FileReadingMessageSource();
        sourceReader.setDirectory(new File(INPUT_DIR));
        sourceReader.setFilter(new SimplePatternFileListFilter(FILE_PATTERN));
        return sourceReader;
    }

    @Bean
    @ServiceActivator (inputChannel = "fileChannel1")
    public MessageHandler fileWritingMessageHandler1 () {
        FileWritingMessageHandler handler =  new FileWritingMessageHandler(new File(OUTPUT_DIR1));
        handler.setFileExistsMode(FileExistsMode.REPLACE);
        handler.setExpectReply(false);
        return handler;
    }



    public static void main(String... args) {
        AbstractApplicationContext context
                = new AnnotationConfigApplicationContext(P2PIntegrationConfig.class);
        context.registerShutdownHook();

        Scanner scanner = new Scanner(System.in);
        System.out.print("Please enter q and press <enter> to exit the program: ");

        while (true) {
            String input = scanner.nextLine();
            if("q".equals(input.trim())) {
                break;
            }
        }
        System.exit(0);
    }
}
