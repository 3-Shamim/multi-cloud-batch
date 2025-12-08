package com.multicloud.batch.service;

import com.multicloud.batch.dto.OrganizationDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class OrganizationService {

    private final JdbcTemplate jdbcTemplate;

    public List<OrganizationDTO> findAllOrganizations() {

        return jdbcTemplate.query(
                "SELECT id, name, internal, exceptional FROM organizations",
                (rs, rowNum) -> new OrganizationDTO(
                        rs.getLong("id"),
                        rs.getString("name"),
                        rs.getBoolean("internal"),
                        rs.getBoolean("exceptional")
                )
        );
    }

    public List<OrganizationDTO> findAllExternalOrganizations() {

        return jdbcTemplate.query(
                "SELECT id, name, internal, exceptional FROM organizations WHERE internal = false",
                (rs, rowNum) -> new OrganizationDTO(
                        rs.getLong("id"),
                        rs.getString("name"),
                        rs.getBoolean("internal"),
                        rs.getBoolean("exceptional")
                )
        );
    }

    public OrganizationDTO findOrganizationById(long id) {

        List<OrganizationDTO> data = jdbcTemplate.query(
                "SELECT id, name, internal, exceptional FROM organizations WHERE id = ?",
                (rs, rowNum) -> new OrganizationDTO(
                        rs.getLong("id"),
                        rs.getString("name"),
                        rs.getBoolean("internal"),
                        rs.getBoolean("exceptional")
                ),
                id
        );

        return CollectionUtils.isEmpty(data) ? null : data.getFirst();
    }

}
