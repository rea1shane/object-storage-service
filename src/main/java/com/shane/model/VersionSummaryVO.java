package com.shane.model;

import com.amazonaws.services.s3.model.S3VersionSummary;
import io.swagger.annotations.ApiModelProperty;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shane
 */
@SuperBuilder
public class VersionSummaryVO extends ObjectSummaryVO {

    private static final long serialVersionUID = 7925120915886096955L;

    @ApiModelProperty("版本 id")
    private String versionId;

    @ApiModelProperty("最终版本标识")
    private boolean isLatest;

    @ApiModelProperty("删除标记")
    private boolean isDeleteMarker;

    public static VersionSummaryVO generateVersionSummaryVO(S3VersionSummary version) {
        if (version.getKey().endsWith("/")) {
            return null;
        }
        VersionSummaryVO vo = VersionSummaryVO.builder()
                .type(Type.VERSION)
                .key(version.getKey())
                .versionId(version.getVersionId())
                .isLatest(version.isLatest())
                .isDeleteMarker(version.isDeleteMarker())
                .lastModified(version.getLastModified())
                .size(version.getSize())
                .build();
        vo.setNameByKey();
        return vo;
    }

    public static List<VersionSummaryVO> generateVersionSummaryVOList(List<S3VersionSummary> versions) {
        List<VersionSummaryVO> vos = new ArrayList<>();
        for (S3VersionSummary version : versions) {
            VersionSummaryVO vo = generateVersionSummaryVO(version);
            if (vo != null) {
                vos.add(vo);
            }
        }
        return vos;
    }

}
