package com.kosta.demo;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Controller
public class KafkaHomeController {
    
    
    private static final DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    private static final String topicName = "MYTOPIC";
    
    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;
    
    @RequestMapping("/kafka.do")
    public void kafka(String msg) {
        LocalDateTime date = LocalDateTime.now();
        String dateStr = date.format(fmt);
        kafkaTemplate.send(topicName, "8080 send " + dateStr + " : " + msg);
    }
    
    @KafkaListener(topics = topicName,groupId = "gid111")
    public void listen(String message) {
        System.out.println("Received Msg MYTOPIC :  " + message);
    }
    

    @KafkaListener(topics = topicName,groupId = "gid222")
    public void listen2(@Headers MessageHeaders headers,@Payload String payload ) {
        System.out.println("Consume Headers : " + headers.toString());
        System.out.println("PayLoad : " + payload);
    }
    
}
