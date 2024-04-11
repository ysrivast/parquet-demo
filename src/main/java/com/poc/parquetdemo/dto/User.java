package com.poc.parquetdemo.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *
 * <User>
 *     <id>1</id>
 *     <username>John</username>
 *     <active>true</active>
 * </User>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {
    private Long id;
    private String username;
    private Boolean active;

    // getters, setters, equals, hashCode
}