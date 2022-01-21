package com.kimutai.SpringBootRabbitMq.Controller;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import com.kimutai.SpringBootRabbitMq.models.Weather;
import com.kimutai.SpringBootRabbitMq.Sender.RabbitMQSender;

import org.apache.logging.log4j.*;
import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.*;
import org.springframework.http.MediaType;
import java.util.stream.Stream;



@RestController("/api")
public class WeatherController{

@Autowired
RabbitMQSender rabbitMQSender;
@Autowired
private Queue queue;
@Autowired
private Weather weather;



private static  Logger logger = LogManager.getLogger(WeatherController.class.toString());


@RequestMapping("/weather")
public Flux<Weather> getWeather() {
//Every 10mins
Flux<Long> interval = Flux.interval(Duration.ofSeconds(600));
WebClient webClient = WebClient.create("https://api.weatherstack.com");
   weather  =  webClient
 .get()
 .uri("/current?access_key=74a8975bbe388788ef1376ac1298d322&query=Nairobi")
.accept(MediaType.TEXT_EVENT_STREAM)
.exchange()
.flatMapMany(clientResponse -> clientResponse.bodyToFlux(Weather.class))
.doOnNext(System.out::println).blockFirst();
//Publishing the results to RabbitMQ
rabbitMQSender.send(weather);
logger.info("Sending Message to the queue: " + weather);
return Flux.zip(weather,interval,(key,value) ->key);
} 



}

