package com.example.spring.kafka.config;

import com.example.spring.kafka.event.TransactionEventJson;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Bytes;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.autoconfigure.kafka.SslBundleSslEngineFactory;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.SeekUtils;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.util.StringUtils;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Spring Kafka configuration for exception handling.
 * <p>
 * We configure a {@link DeadLetterPublishingRecoverer} to handle messages that fail processing
 * by publishing them to a dead letter topic (DLT). This prevents consumer blocking and allows
 * for later analysis and reprocessing of failed messages.
 * <p>
 * The configuration supports two scenarios:
 * <ul>
 *   <li><b>Deserialization exceptions</b> - Messages that cannot be deserialized are sent to DLT
 *       using {@link ByteArraySerializer} since we don't know the original format</li>
 *   <li><b>Processing exceptions</b> - Successfully deserialized messages that fail during
 *       business logic processing are sent to DLT with proper serialization</li>
 * </ul>
 */
@Configuration
public class KafkaExceptionHandlingConfiguration {

    private final KafkaProperties properties;

    public KafkaExceptionHandlingConfiguration(KafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    public DeadLetterPublishingRecoverer recoverer(KafkaTemplate<?, ?> bytesKafkaTemplate, KafkaTemplate<?, ?> kafkaTemplate) {
        Map<Class<?>, KafkaOperations<? extends Object, ? extends Object>> templates = new LinkedHashMap<>();
        templates.put(TransactionEventJson.class, kafkaTemplate);
        templates.put(byte[].class, bytesKafkaTemplate);
        return new DeadLetterPublishingRecoverer(templates);
    }

    /**
     * In case you don't specify a {@link org.springframework.util.backoff.BackOff}
     * the {@link SeekUtils#DEFAULT_BACK_OFF} will be configured.
     * <p>
     * It's a {@link FixedBackOff} with 0 interval and will try 10 times.
     */
    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
        return new DefaultErrorHandler(recoverer, new FixedBackOff(0, 5));
    }

    /**
     * This is the specific Producer for serialization exceptions.
     * We configure ByteArraySerializer for both the key and value serializer.
     * Because we don't know upfront in what 'format' the record from the topic caused the deserialization exception.
     */
    @Bean
    public ProducerFactory<Bytes, Bytes> bytesProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties();
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new DefaultKafkaProducerFactory<>(producerProperties);
    }

    /**
     * This is the specific Kafka template for serialization exceptions.
     */
    @Bean
    public KafkaTemplate<Bytes, Bytes> bytesKafkaTemplate(ProducerFactory<Bytes, Bytes> bytesProducerFactory) {
        return new KafkaTemplate<>(bytesProducerFactory);
    }

    /**
     * We have to also create the "default" kafkaProducerFactory.
     * The code is basically copied from: {@link org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration#kafkaProducerFactory(KafkaConnectionDetails, ObjectProvider)}
     */
    @Bean
    DefaultKafkaProducerFactory<?, ?> kafkaProducerFactory(KafkaConnectionDetails connectionDetails,
                                                           ObjectProvider<DefaultKafkaProducerFactoryCustomizer> customizers) {
        Map<String, Object> properties = this.properties.buildProducerProperties();
        applyKafkaConnectionDetailsForProducer(properties, connectionDetails);
        DefaultKafkaProducerFactory<?, ?> factory = new DefaultKafkaProducerFactory<>(properties);
        String transactionIdPrefix = this.properties.getProducer().getTransactionIdPrefix();
        if (transactionIdPrefix != null) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }
        customizers.orderedStream().forEach((customizer) -> customizer.customize(factory));
        return factory;
    }

    /**
     * We have to also create the "default" kafkaTemplate.
     * The code is basically a copy from:
     * {@link org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration#kafkaTemplate(ProducerFactory, ProducerListener, ObjectProvider)}
     */
    @Bean
    public KafkaTemplate<?, ?> kafkaTemplate(ProducerFactory<Object, Object> kafkaProducerFactory,
                                             ProducerListener<Object, Object> kafkaProducerListener,
                                             ObjectProvider<RecordMessageConverter> messageConverter) {
        KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
        messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
        kafkaTemplate.setProducerListener(kafkaProducerListener);
        kafkaTemplate.setDefaultTopic(this.properties.getTemplate().getDefaultTopic());
        return kafkaTemplate;
    }

    private void applyKafkaConnectionDetailsForProducer(Map<String, Object> properties,
                                                        KafkaConnectionDetails connectionDetails) {
        KafkaConnectionDetails.Configuration producer = connectionDetails.getProducer();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producer.getBootstrapServers());
        applySecurityProtocol(properties, producer.getSecurityProtocol());
        applySslBundle(properties, producer.getSslBundle());
    }

    static void applySslBundle(Map<String, Object> properties, SslBundle sslBundle) {
        if (sslBundle != null) {
            properties.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, SslBundleSslEngineFactory.class);
            properties.put(SslBundle.class.getName(), sslBundle);
        }
    }

    static void applySecurityProtocol(Map<String, Object> properties, String securityProtocol) {
        if (StringUtils.hasLength(securityProtocol)) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        }
    }
}
