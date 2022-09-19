package com.shane.model;

import io.swagger.annotations.ApiModelProperty;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;

/**
 * @author shane
 */
@SuperBuilder
public class CommonSummary implements Serializable {

    private static final long serialVersionUID = -7215579777327032067L;

    @ApiModelProperty("名称")
    private String name;

    @ApiModelProperty("key")
    private String key;

    @ApiModelProperty("类型")
    private Type type;

    /**
     * 通过 key 转换为名称
     */
    public void setNameByKey() {
        String[] strings = key.split("/");
        name = strings[strings.length - 1];
        if (type == Type.DIRECTORY) {
            name += "/";
        }
    }

    public enum Type {
        OBJECT, VERSION, DIRECTORY
    }

}
