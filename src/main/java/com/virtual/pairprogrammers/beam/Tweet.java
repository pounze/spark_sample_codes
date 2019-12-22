package com.virtual.pairprogrammers.beam;

import lombok.Data;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.Serializable;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet implements Serializable
{
    @JsonProperty("full text")
    private String fulltext;
    private String lang;
    private User user;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public class User implements Serializable
    {
        private Long id;
        private String name;
        @JsonProperty("screen_name")
        private String screenName;
    }

}
