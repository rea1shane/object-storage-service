package com.shane.model;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.swagger.annotations.ApiModelProperty;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author shane
 */
@SuperBuilder
public class ObjectSummaryVO extends CommonSummary {

    private static final long serialVersionUID = 7289515651458023774L;

    @ApiModelProperty("上次修改时间")
    private Date lastModified;

    @ApiModelProperty("文件大小，单位 bytes")
    private long size;

    public static ObjectSummaryVO generateObjectSummaryVO(S3ObjectSummary object) {
        if (object.getKey().endsWith("/")) {
            return null;
        }
        ObjectSummaryVO vo = ObjectSummaryVO.builder()
                .type(Type.OBJECT)
                .key(object.getKey())
                .lastModified(object.getLastModified())
                .size(object.getSize())
                .build();
        vo.setNameByKey();
        return vo;
    }

    public static List<ObjectSummaryVO> generateObjectSummaryVOList(List<S3ObjectSummary> objects) {
        List<ObjectSummaryVO> vos = new ArrayList<>();
        for (S3ObjectSummary object : objects) {
            ObjectSummaryVO vo = generateObjectSummaryVO(object);
            if (vo != null) {
                vos.add(vo);
            }
        }
        return vos;
    }

}
