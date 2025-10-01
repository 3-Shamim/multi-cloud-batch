package com.multicloud.batch.dao.huawei.payload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record HuaweiAuthRequest(Auth auth) {

    public record Auth(Identity identity, Scope scope) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Identity(List<String> methods, Password password, AssumeRole assume_role) {
    }

    public record Password(User user) {
    }

    public record User(String name, String password, Domain domain) {
    }

    public record Domain(String name) {
    }

    public record AssumeRole(String domain_name, String agency_name) {
    }

    public record Scope(Project project) {
    }

    public record Project(String name) {
    }

    public static HuaweiAuthRequest buildPasswordIdentity(String username, String password, String domain, String project) {

        HuaweiAuthRequest.User user = new HuaweiAuthRequest.User(
                username, password, new HuaweiAuthRequest.Domain(domain)
        );

        return new HuaweiAuthRequest(
                new HuaweiAuthRequest.Auth(
                        new HuaweiAuthRequest.Identity(
                                List.of("password"),
                                new HuaweiAuthRequest.Password(user),
                                null
                        ),
                        new HuaweiAuthRequest.Scope(
                                new HuaweiAuthRequest.Project(project)
                        )
                )
        );
    }


    public static HuaweiAuthRequest buildAssumeRoleIdentity(String domainName, String agencyName, String project) {

        return new HuaweiAuthRequest(
                new HuaweiAuthRequest.Auth(
                        new HuaweiAuthRequest.Identity(
                                List.of("assume_role"),
                                null,
                                new HuaweiAuthRequest.AssumeRole(domainName, agencyName)
                        ),
                        new HuaweiAuthRequest.Scope(
                                new HuaweiAuthRequest.Project(project)
                        )
                )
        );
    }

}
