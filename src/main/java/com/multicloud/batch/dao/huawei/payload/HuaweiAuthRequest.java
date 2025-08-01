package com.multicloud.batch.dao.huawei.payload;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record HuaweiAuthRequest(Auth auth) {

    public record Auth(Identity identity, Scope scope) {
    }

    public record Identity(List<String> methods, Password password) {
    }

    public record Password(User user) {
    }

    public record User(String name, String password, Domain domain) {
    }

    public record Domain(String name) {
    }

    public record Scope(Project project) {
    }

    public record Project(String name) {
    }

    public static HuaweiAuthRequest build(String project, String domain, String username, String password) {

        HuaweiAuthRequest.User user = new HuaweiAuthRequest.User(
                username, password, new HuaweiAuthRequest.Domain(domain)
        );

        return new HuaweiAuthRequest(
                new HuaweiAuthRequest.Auth(
                        new HuaweiAuthRequest.Identity(
                                List.of("password"),
                                new HuaweiAuthRequest.Password(user)
                        ),
                        new HuaweiAuthRequest.Scope(
                                new HuaweiAuthRequest.Project(project)
                        )
                )
        );
    }

}
