package com.poc.parquetdemo.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

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
@XmlRootElement(name="User")
public class User {
    @XmlElement(name = "Id")
    private Long id;
    @XmlElement(name = "Username")
    private String username;
    @XmlElement(name = "Active")
    private Boolean active;

    // getters, setters, equals, hashCode
}