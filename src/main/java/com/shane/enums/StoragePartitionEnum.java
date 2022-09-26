package com.shane.enums;

public enum StoragePartitionEnum {

    DATA("data/", true),

    MODEL("model/", true),
    MODEL_REGISTERED("model-registered/", false),

    ;

    private final String path;
    private final boolean isUserVisible;

    StoragePartitionEnum(String path, boolean isUserVisible) {
        this.path = path;
        this.isUserVisible = isUserVisible;
    }

    public String getPath() {
        return path;
    }

    public boolean isUserVisible() {
        return isUserVisible;
    }

}
