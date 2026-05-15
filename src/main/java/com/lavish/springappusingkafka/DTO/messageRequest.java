package com.lavish.springappusingkafka.DTO;


import lombok.Data;

@Data
public class messageRequest {
    private String messagecontent;
    private String sender;
}
