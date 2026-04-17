package com.kayledger.api.payment.model;

import java.util.List;

public record PaymentIntentDetails(PaymentIntent paymentIntent, List<PaymentAttempt> attempts) {
}
