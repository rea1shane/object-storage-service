package com.shane.model;

import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shane
 */
@SuperBuilder
public class DirectorySummaryVO extends CommonSummary {

    private static final long serialVersionUID = 7462671806072585008L;

    public static DirectorySummaryVO generateDirectorySummaryVO(String key) {
        if (!key.endsWith("/")) {
            return null;
        }
        DirectorySummaryVO vo = DirectorySummaryVO.builder()
                .type(Type.DIRECTORY)
                .key(key)
                .build();
        vo.setNameByKey();
        return vo;
    }

    public static List<DirectorySummaryVO> generateDirectorySummaryVOList(List<String> keys) {
        List<DirectorySummaryVO> vos = new ArrayList<>();
        for (String key : keys) {
            DirectorySummaryVO vo = generateDirectorySummaryVO(key);
            if (vo != null) {
                vos.add(vo);
            }
        }
        return vos;
    }

}
