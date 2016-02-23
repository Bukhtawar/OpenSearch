/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.role;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Builder for requests to retrieve a role from the shield index
 */
public class GetRolesRequestBuilder extends ActionRequestBuilder<GetRolesRequest, GetRolesResponse, GetRolesRequestBuilder> {

    public GetRolesRequestBuilder(ElasticsearchClient client) {
        this(client, GetRolesAction.INSTANCE);
    }

    public GetRolesRequestBuilder(ElasticsearchClient client, GetRolesAction action) {
        super(client, action, new GetRolesRequest());
    }

    public GetRolesRequestBuilder names(String... roles) {
        request.roles(roles);
        return this;
    }
}
