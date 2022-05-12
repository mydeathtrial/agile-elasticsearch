package com.agile;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(indexName = "persons7")
public class Persons {
    @Id
    @Field(type = FieldType.Keyword, index = false, store = true)
    private String id;
    @Field(type = FieldType.Integer, index = false, store = true)
    private Integer age;
    @Field(type = FieldType.Text, index = true, store = true)
    private String name;
    @Field(type = FieldType.Boolean, index = true, store = true)
    private Boolean sex;
}