package de.segoy.udemy.kafka.streams;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Transaction {

    String name;
    int amount;
   Long time;


}
