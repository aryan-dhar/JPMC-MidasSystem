package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.service.IncentiveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    private final DatabaseConduit databaseConduit;
    private final IncentiveService incentiveService;

    public TransactionListener(DatabaseConduit databaseConduit, IncentiveService incentiveService) {
        this.databaseConduit = databaseConduit;
        this.incentiveService = incentiveService;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        // Validate sender and recipient
        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            logger.warn("Invalid transaction: sender or recipient not found");
            return;
        }

        // Calculate incentive
        float incentive = incentiveService.getIncentiveAmount(transaction);
        transaction.setIncentive(incentive);
        logger.info("Calculated incentive: {}", incentive);

        // Process transaction
        boolean success = databaseConduit.processTransaction(sender, recipient, transaction.getAmount(), transaction.getIncentive());
        if (success) {
            logger.info("Transaction processed successfully");
        } else {
            logger.warn("Transaction failed: insufficient funds");
        }
    }
} 