package io.eventador;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.common.security.scram.ScramSaslClient;

import java.util.Properties;
import java.util.UUID;

public class FlinkReadKafkaSASL {
        public static void main(String[] args) throws Exception {
            // Read parameters from command line
            final ParameterTool params = ParameterTool.fromArgs(args);

            if(params.getNumberOfParameters() < 5) {
                System.out.println("\nUsage: FlinkReadKafkaSASL --read-topic <topic> --bootstrap.servers <kafka brokers> --group.id <groupid> --username <username> --password <password>");
                return;
            }

            System.out.println("Lets do this!");
            // Configure ScramLogin via jaas
            //String module = "org.apache.kafka.common.security.scram.ScramLoginModule";
            String module = "org.apache.kafka.common.security.plain.PlainLoginModule";
            String jaasConfig = String.format("%s required username=\"%s\" password=\"%s\";", module, params.getRequired("username"), params.getRequired("password"));

            // Flink and Kafka parameters
            Properties kparams = params.getProperties();
            kparams.setProperty("auto.offset.reset", "earliest");
            kparams.setProperty("flink.starting-position", "earliest");
            //kparams.setProperty("group.id", params.getRequired("group.id"));
            kparams.setProperty("group.id", "erik_consumer_" + Double.toString(Math.random()));
            kparams.setProperty("bootstrap.servers", params.getRequired("bootstrap.servers"));;

            // SASL parameters
            kparams.setProperty("security.protocol", "SASL_SSL");
            //kparams.setProperty("sasl.mechanism", "SCRAM-SHA-256");
            kparams.setProperty("sasl.mechanism", "PLAIN");
            kparams.setProperty("sasl.jaas.config", jaasConfig);

            // Setup streaming environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
            env.enableCheckpointing(300000); // 300 seconds
            env.getConfig().setGlobalJobParameters(params);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.setParallelism(1);

            DataStream<String> messageStream = env
                    .addSource(new FlinkKafkaConsumer011<>(
                            params.getRequired("read-topic"),
                            new SimpleStringSchema(),
                            kparams));

            // Print Kafka messages to stdout - will be visible in logs
            messageStream.print();

            env.execute("FlinkReadKafkaSASL");
        }
}
