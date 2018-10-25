package com.kafkaProducer;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.Date;
import java.util.Calendar;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/*
Usage: java -jar target/kafka-avro-producer-1.0-SNAPSHOT.jar  -confPath conf/conf.yaml
 */

public class KafkaAvroProducer {

    public static void main(String[] args){

        Options opts = new Options();
        opts.addOption("confPath", true, "Path to the config file.");

        CommandLine cmd = null;

        try {
            CommandLineParser parser = new BasicParser();
            cmd = parser.parse(opts, args);
        } catch (Exception e){
            e.printStackTrace();
            System.out.println("Parameter parsing exception" + e.getMessage());

        }

        String configPath = cmd.getOptionValue("confPath");

        final Map commonConfig = Utils.findAndReadConfigFile(configPath, true);

        String kafkaTopic = (String)commonConfig.get("kafka.topic");
        String kafkaBrokers = (String)commonConfig.get("kafka.brokers");

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");


        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        String customerid = "9876543212";
        String name = "Amanda Simon";
        String street = "Rubio";
        String city = "Escondido";
        String state = "CA";
        String zip = "90124";
        float salesprice = 100.0f;
        long epoch = 0;

        try {
            Date today = Calendar.getInstance().getTime();
            // Constructs a SimpleDateFormat using the given pattern
            SimpleDateFormat simpleFormat = new SimpleDateFormat("MMM dd yyyy HH:mm:ss.SSS zzz");
            // format() formats a Date into a date/time string.
            String currentTime = simpleFormat.format(today);
            System.out.println("Current Time = " + currentTime);
            try {
                // parse() parses text from the beginning of the given string to produce a date.
                Date date = simpleFormat.parse(currentTime);
                // getTime() returns the epoch date
                epoch = date.getTime();
                System.out.println("Current Time in Epoch: " + epoch);

            } catch (ParseException e) {
                e.printStackTrace();
            }

            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);

            DatumWriter<StateSales> writer = new SpecificDatumWriter<>(StateSales.getClassSchema());
            StateSales stateSalesMsg = new StateSales();

            stateSalesMsg.setCustomerid(customerid);
            stateSalesMsg.setName(name);
            stateSalesMsg.setStreet(street);
            stateSalesMsg.setCity(city);
            stateSalesMsg.setState(state);
            stateSalesMsg.setZip(zip);
            stateSalesMsg.setSalesprice(salesprice);
            stateSalesMsg.setTimestamp(epoch);

            writer.write(stateSalesMsg, encoder);
            encoder.flush();
            outStream.close();

            byte[] serializedBytes = outStream.toByteArray();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(kafkaTopic, "state-sales", serializedBytes);
            Future<RecordMetadata> result = producer.send(record);
            try {
                RecordMetadata recordMetadata = result.get();
                System.out.println(": Record Metadata:");
                System.out.println("\t    Topic: " + recordMetadata.topic());
                System.out.println("\tTimestamp:" + recordMetadata.timestamp());
                System.out.println("\tPartition:" + recordMetadata.partition());
                System.out.println("\t   Offset:" + recordMetadata.offset());
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw e;
            } catch (ExecutionException e) {
                e.printStackTrace();
                throw e;
            }

            Thread.sleep(250);

        } catch (Exception e){
            e.printStackTrace();
        }
        producer.close();

    }



}
