/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { SyncRepository } from '../models/SyncRepository';
import type { SyncRepositoryResponse } from '../models/SyncRepositoryResponse';

import type { CancelablePromise } from '../core/CancelablePromise';
import { OpenAPI } from '../core/OpenAPI';
import { request as __request } from '../core/request';

export class IndexifyService {

    /**
     * @param requestBody
     * @returns SyncRepositoryResponse Repository synced successfully
     * @throws ApiError
     */
    public static syncRepository(
        requestBody: SyncRepository,
    ): CancelablePromise<SyncRepositoryResponse> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/repository/sync',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                500: `Unable to sync repository`,
            },
        });
    }

}
